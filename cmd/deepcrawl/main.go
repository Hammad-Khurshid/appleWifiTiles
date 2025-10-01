package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"wloc/lib"

	"github.com/leaanthony/clir"
)

const (
	processedBSSIDsFile = "processed_bssids.txt"
	outputDir           = "deep_crawl_results"
	recordsPerFile      = 1000000
	maxRetries          = 3
	numWorkers          = 50
)

type WiFiRecord struct {
	BSSID     string  `json:"bssid"`
	Timestamp int64   `json:"timestamp"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type AP struct {
	BSSID     string
	Latitude  float64
	Longitude float64
}

func main() {
	cli := clir.NewCli("deepcrawl", "Deep crawl BSSIDs using wloc API", "0.0.1")

	cli.Action(func() error {
		startDeepCrawl()
		return nil
	})

	if err := cli.Run(); err != nil {
		log.Fatal(err)
	}
}

func startDeepCrawl() {
	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
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

	log.Printf("Starting deep crawl with %d BSSIDs to process", len(queue))

	// Start crawling
	crawlBSSIDs(queue, processedBSSIDs)
}

func getCurrentFileNumber() int {
	files, err := ioutil.ReadDir(outputDir)
	if err != nil {
		return 0
	}

	maxFile := 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		var fileNum int
		if _, err := fmt.Sscanf(file.Name(), "%dmillion.txt", &fileNum); err == nil {
			if fileNum > maxFile {
				maxFile = fileNum
			}
		}
	}
	return maxFile
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

func loadSeedBSSIDs() ([]AP, error) {
	file, err := os.Open("us_aps.txt")
	if err != nil {
		return nil, fmt.Errorf("failed to open seed BSSIDs file: %w", err)
	}
	defer file.Close()

	var seedBSSIDs []AP
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

		seedBSSIDs = append(seedBSSIDs, AP{
			BSSID:     bssid,
			Latitude:  lat,
			Longitude: lon,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading seed BSSIDs file: %w", err)
	}

	log.Printf("Loaded %d seed BSSIDs", len(seedBSSIDs))
	return seedBSSIDs, nil
}

func createBSSIDQueue(seedBSSIDs []AP, processedBSSIDs map[string]struct{}) []AP {
	var queue []AP

	for _, ap := range seedBSSIDs {
		if _, processed := processedBSSIDs[ap.BSSID]; !processed {
			queue = append(queue, ap)
		}
	}

	return queue
}

func crawlBSSIDs(queue []AP, processedBSSIDs map[string]struct{}) {
	// Create channels
	bssidChan := make(chan AP, numWorkers)
	resultsChan := make(chan []WiFiRecord, numWorkers)
	processedChan := make(chan string, numWorkers)

	var wg sync.WaitGroup
	var processedMutex sync.Mutex
	var totalRecords int64

	// Start writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		currentFileNum := getCurrentFileNumber()
		var currentFile *os.File
		var recordsInCurrentFile int
		var err error

		for results := range resultsChan {
			// Check if we need a new file
			if currentFile == nil || recordsInCurrentFile >= recordsPerFile {
				if currentFile != nil {
					currentFile.Close()
				}
				currentFileNum++
				filename := fmt.Sprintf("%s/%dmillion.txt", outputDir, currentFileNum)
				currentFile, err = os.Create(filename)
				if err != nil {
					log.Printf("Failed to create file %s: %v", filename, err)
					continue
				}
				recordsInCurrentFile = 0
				log.Printf("Created new file: %s", filename)
			}

			// Write records to current file
			for _, record := range results {
				data, err := json.Marshal(record)
				if err != nil {
					log.Printf("Failed to marshal record: %v", err)
					continue
				}
				if _, err := currentFile.WriteString(string(data) + "\n"); err != nil {
					log.Printf("Failed to write record: %v", err)
					continue
				}
				recordsInCurrentFile++
				totalRecords++
			}
		}

		if currentFile != nil {
			currentFile.Close()
		}
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
					records = append(records, WiFiRecord{
						BSSID:     neighbor.BSSID,
						Timestamp: time.Now().UnixMilli(),
						Latitude:  neighbor.Latitude,
						Longitude: neighbor.Longitude,
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

	// Feed BSSIDs to workers
	for _, ap := range queue {
		bssidChan <- ap
	}
	close(bssidChan)

	// Wait for workers to finish
	wg.Wait()
	close(resultsChan)
	close(processedChan)

	log.Printf("Deep crawl completed! Total records: %d", totalRecords)
}

func getNeighborsWithRetry(bssid string, retries int) ([]AP, error) {
	var err error
	var neighbors []AP

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

func getNeighbors(bssid string) ([]AP, error) {
	blocks, err := lib.QueryBssid([]string{bssid}, 0)
	if err != nil {
		return nil, err
	}

	var neighbors []AP
	for _, block := range blocks {
		neighbors = append(neighbors, AP{
			BSSID:     block.BSSID,
			Latitude:  block.Location.Lat,
			Longitude: block.Location.Long,
		})
	}

	return neighbors, nil
}
