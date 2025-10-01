package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"wloc/lib"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/leaanthony/clir"
)

const (
	processedBSSIDsFile = "processed_bssids.txt"
	tableName           = "wifi-bssids-table-stg-us-east-1"
	region              = "us-east-1"
	maxRetries          = 3
	numWorkers          = 50
	batchSize           = 25 // DynamoDB batch write limit
)

type WiFiRecord struct {
	BSSID              string  `json:"bssid" dynamodbav:"bssid"`
	Timestamp          int64   `json:"timestamp" dynamodbav:"timestamp"`
	Latitude           float64 `json:"latitude" dynamodbav:"latitude"`
	Longitude          float64 `json:"longitude" dynamodbav:"longitude"`
	HorizontalAccuracy int     `json:"horizontal_accuracy" dynamodbav:"horizontal_accuracy"`
	VerticalAccuracy   int     `json:"vertical_accuracy" dynamodbav:"vertical_accuracy"`
}

type DynamoDBItem struct {
	BSSID              string  `dynamodbav:"bssid"`
	Timestamp          int64   `dynamodbav:"timestamp"`
	Latitude           float64 `dynamodbav:"latitude"`
	Longitude          float64 `dynamodbav:"longitude"`
	HorizontalAccuracy int     `dynamodbav:"horizontal_accuracy"`
	VerticalAccuracy   int     `dynamodbav:"vertical_accuracy"`
}

func main() {
	cli := clir.NewCli("awsdeepcrawl", "Deep crawl BSSIDs using wloc API and store in DynamoDB", "0.0.1")

	cli.Action(func() error {
		startAWSDeepCrawl()
		return nil
	})

	if err := cli.Run(); err != nil {
		log.Fatal(err)
	}
}

func startAWSDeepCrawl() {
	// Initialize AWS session
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		log.Fatalf("Failed to create AWS session: %v", err)
	}

	dynamoClient := dynamodb.New(sess)

	// Test DynamoDB connection
	if err := testDynamoDBConnection(dynamoClient); err != nil {
		log.Fatalf("Failed to connect to DynamoDB: %v", err)
	}

	// Load processed BSSIDs
	processedBSSIDs, err := loadProcessedBSSIDs()
	if err != nil {
		log.Fatalf("Failed to load processed BSSIDs: %v", err)
	}

	// Load seed BSSIDs from backup file
	seedBSSIDs, err := loadSeedBSSIDs()
	if err != nil {
		log.Fatalf("Failed to load seed BSSIDs: %v", err)
	}

	// Create queue of BSSIDs to process
	queue := createBSSIDQueue(seedBSSIDs, processedBSSIDs)

	if len(queue) == 0 {
		log.Println("No new BSSIDs to process. All done!")
		return
	}

	log.Printf("Starting AWS deep crawl with %d BSSIDs to process", len(queue))
	log.Printf("Already processed %d BSSIDs", len(processedBSSIDs))
	log.Printf("Target DynamoDB table: %s", tableName)

	// Start crawling
	crawlBSSIDsToDynamoDB(queue, processedBSSIDs, dynamoClient)
}

func testDynamoDBConnection(client *dynamodb.DynamoDB) error {
	log.Printf("Testing connection to DynamoDB table: %s", tableName)

	_, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		return fmt.Errorf("failed to describe table %s: %w", tableName, err)
	}

	log.Println("✅ DynamoDB connection successful")
	return nil
}

func loadProcessedBSSIDs() (map[string]struct{}, error) {
	processed := make(map[string]struct{})

	if _, err := os.Stat(processedBSSIDsFile); os.IsNotExist(err) {
		log.Println("No processed BSSIDs file found. Starting fresh.")
		return processed, nil
	}

	file, err := os.Open(processedBSSIDsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open processed BSSIDs file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		bssid := strings.TrimSpace(scanner.Text())
		if bssid != "" {
			processed[bssid] = struct{}{}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading processed BSSIDs file: %w", err)
	}

	log.Printf("Loaded %d processed BSSIDs", len(processed))
	return processed, nil
}

func loadSeedBSSIDs() ([]lib.AP, error) {
	file, err := os.Open("us_aps.txt")
	if err != nil {
		return nil, fmt.Errorf("failed to open seed BSSIDs file: %w", err)
	}
	defer file.Close()

	var seedBSSIDs []lib.AP
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Parse line: "MAC: aa:bb:cc:dd:ee:ff - 27.123456 -82.654321"
		// Use regex to extract the three parts
		re := regexp.MustCompile(`^MAC: ([a-fA-F0-9:]+) - (-?\d+\.\d+) -(-?\d+\.\d+)$`)
		matches := re.FindStringSubmatch(line)
		if len(matches) != 4 {
			log.Printf("Skipping malformed line: %s", line)
			continue
		}

		bssid := matches[1]
		latStr := matches[2]
		lonStr := matches[3]

		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			log.Printf("Failed to parse latitude in line: %s, error: %v", line, err)
			continue
		}
		lon, err := strconv.ParseFloat(lonStr, 64)
		if err != nil {
			log.Printf("Failed to parse longitude in line: %s, error: %v", line, err)
			continue
		}

		seedBSSIDs = append(seedBSSIDs, lib.AP{
			BSSID: bssid,
			Location: lib.Location{
				Long:               lon,
				Lat:                lat,
				Alt:                0, // We don't have altitude data from the seed file
				HorizontalAccuracy: 0, // We don't have accuracy data from the seed file
				VerticalAccuracy:   0, // We don't have accuracy data from the seed file
			},
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading seed BSSIDs file: %w", err)
	}

	log.Printf("Loaded %d seed BSSIDs", len(seedBSSIDs))
	return seedBSSIDs, nil
}

func createBSSIDQueue(seedBSSIDs []lib.AP, processedBSSIDs map[string]struct{}) []lib.AP {
	var queue []lib.AP

	for _, ap := range seedBSSIDs {
		if _, processed := processedBSSIDs[ap.BSSID]; !processed {
			queue = append(queue, ap)
		}
	}

	return queue
}

func crawlBSSIDsToDynamoDB(queue []lib.AP, processedBSSIDs map[string]struct{}, dynamoClient *dynamodb.DynamoDB) {
	// Create channels
	bssidChan := make(chan lib.AP, numWorkers)
	resultsChan := make(chan []WiFiRecord, numWorkers)
	processedChan := make(chan string, numWorkers)

	var wg sync.WaitGroup
	var processedMutex sync.Mutex

	// Start DynamoDB writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		writeToDynamoDB(resultsChan, dynamoClient)
	}()

	// Start processed BSSID writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		processedFile, err := os.OpenFile(processedBSSIDsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Failed to open processed BSSIDs file: %v", err)
			return
		}
		defer processedFile.Close()

		for bssid := range processedChan {
			processedMutex.Lock()
			processedBSSIDs[bssid] = struct{}{}
			processedMutex.Unlock()

			if _, err := processedFile.WriteString(bssid + "\n"); err != nil {
				log.Printf("Error writing processed BSSID: %v", err)
			}
		}
	}()

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ap := range bssidChan {
				neighbors, err := getNeighborsWithRetry(ap.BSSID, maxRetries)
				if err != nil {
					log.Printf("Failed to get neighbors for %s: %v", ap.BSSID, err)
					processedChan <- ap.BSSID
					continue
				}

				var records []WiFiRecord
				for _, neighbor := range neighbors {
					// Convert accuracy to integer meters (Apple returns accuracy in meters)
					horizontalAccuracyMeters := int(neighbor.Location.HorizontalAccuracy)
					verticalAccuracyMeters := int(neighbor.Location.VerticalAccuracy)

					records = append(records, WiFiRecord{
						BSSID:              neighbor.BSSID,
						Timestamp:          time.Now().UnixMilli(),
						Latitude:           neighbor.Location.Lat,
						Longitude:          neighbor.Location.Long,
						HorizontalAccuracy: horizontalAccuracyMeters,
						VerticalAccuracy:   verticalAccuracyMeters,
					})
				}

				if len(records) > 0 {
					resultsChan <- records
					log.Printf("Found %d neighbors for %s", len(records), ap.BSSID)
				}

				processedChan <- ap.BSSID
			}
		}()
	}

	// Send BSSIDs to workers
	go func() {
		for _, ap := range queue {
			bssidChan <- ap
		}
		close(bssidChan)
	}()

	// Wait for all workers to complete
	wg.Wait()
	close(resultsChan)
	close(processedChan)

	log.Printf("AWS deep crawl completed!")
}

func writeToDynamoDB(resultsChan <-chan []WiFiRecord, dynamoClient *dynamodb.DynamoDB) {
	var batch []*dynamodb.WriteRequest
	totalWritten := 0

	for records := range resultsChan {
		for _, record := range records {
			// Convert to DynamoDB item
			item, err := dynamodbattribute.MarshalMap(DynamoDBItem{
				BSSID:              record.BSSID,
				Timestamp:          record.Timestamp,
				Latitude:           record.Latitude,
				Longitude:          record.Longitude,
				HorizontalAccuracy: record.HorizontalAccuracy,
				VerticalAccuracy:   record.VerticalAccuracy,
			})
			if err != nil {
				log.Printf("Failed to marshal record %s: %v", record.BSSID, err)
				continue
			}

			batch = append(batch, &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: item,
				},
			})

			// Write batch when it reaches the limit
			if len(batch) >= batchSize {
				if err := writeBatchToDynamoDB(batch, dynamoClient); err != nil {
					log.Printf("Failed to write batch to DynamoDB: %v", err)
				} else {
					totalWritten += len(batch)
					log.Printf("Written %d records to DynamoDB (total: %d)", len(batch), totalWritten)
				}
				batch = batch[:0] // Reset batch
			}
		}
	}

	// Write remaining items in batch
	if len(batch) > 0 {
		if err := writeBatchToDynamoDB(batch, dynamoClient); err != nil {
			log.Printf("Failed to write final batch to DynamoDB: %v", err)
		} else {
			totalWritten += len(batch)
			log.Printf("Written final batch of %d records to DynamoDB (total: %d)", len(batch), totalWritten)
		}
	}

	log.Printf("DynamoDB writer completed. Total records written: %d", totalWritten)
}

func writeBatchToDynamoDB(batch []*dynamodb.WriteRequest, dynamoClient *dynamodb.DynamoDB) error {
	ctx := context.Background()

	for attempt := 0; attempt < maxRetries; attempt++ {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				tableName: batch,
			},
		}

		result, err := dynamoClient.BatchWriteItemWithContext(ctx, input)
		if err != nil {
			log.Printf("DynamoDB batch write attempt %d failed: %v", attempt+1, err)
			if attempt < maxRetries-1 {
				time.Sleep(time.Second * time.Duration(1<<attempt)) // Exponential backoff
				continue
			}
			return fmt.Errorf("failed to write batch after %d attempts: %w", maxRetries, err)
		}

		// Check for unprocessed items
		if len(result.UnprocessedItems) > 0 {
			log.Printf("Some items were unprocessed, retrying...")
			// Update batch with unprocessed items
			batch = result.UnprocessedItems[tableName]
			if len(batch) == 0 {
				break
			}
			time.Sleep(time.Second * time.Duration(1<<attempt))
			continue
		}

		return nil // Success
	}

	return fmt.Errorf("failed to write all items in batch after %d attempts", maxRetries)
}

func getNeighborsWithRetry(bssid string, retries int) ([]lib.AP, error) {
	var err error
	var neighbors []lib.AP

	for attempt := 0; attempt < retries; attempt++ {
		neighbors, err = getNeighbors(bssid)
		if err == nil {
			return neighbors, nil
		}

		log.Printf("Attempt %d failed for %s: %v", attempt+1, bssid, err)
		if attempt < retries-1 {
			time.Sleep(time.Second * time.Duration(1<<attempt))
		}
	}

	return nil, fmt.Errorf("failed after %d attempts: %w", retries, err)
}

func getNeighbors(bssid string) ([]lib.AP, error) {
	blocks, err := lib.QueryBssid([]string{bssid}, 0)
	if err != nil {
		return nil, err
	}

	return blocks, nil
}
