package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"wloc/lib"
	"wloc/lib/morton"

	"github.com/leaanthony/clir"
)

const (
	allTilesFile         = "us_tiles.json"
	processedTilesFile   = "processed_tiles.txt"
	outputDataFile       = "us_aps.txt"
	failedTilesFile      = "failed_tiles.txt"
	failedTilesRetryFile = "failed_tiles_retry.txt"
	maxRetries           = 5
	numWorkers           = 100
)

type BoundingBox struct {
	MinLat, MaxLat, MinLon, MaxLon, Step float64
}

func main() {
	cli := clir.NewCli("regioncrawl", "Crawl for WiFi APs", "0.0.1")

	// Bounding box for the continental US
	usBbox := BoundingBox{
		MinLat: 24.396308,
		MaxLat: 49.384358,
		MinLon: -125.000000,
		MaxLon: -66.934570,
		Step:   0.01,
	}

	runCmd := cli.NewSubCommand("run", "Run the main US crawl")
	runCmd.Action(func() error {
		crawlUS(usBbox)
		return nil
	})

	retryCmd := cli.NewSubCommand("retry-failed", "Retry processing tiles from failed_tiles.txt")
	retryCmd.Action(func() error {
		retryFailed()
		return nil
	})

	genCmd := cli.NewSubCommand("generate-tiles", "Generate a fresh list of US tiles")
	genCmd.Action(func() error {
		if _, err := os.Stat(allTilesFile); err == nil {
			return fmt.Errorf("%s already exists. Please rename or delete it before generating a new one", allTilesFile)
		}
		generateTiles(usBbox)
		return nil
	})

	if err := cli.Run(); err != nil {
		log.Fatal(err)
	}
}

func generateTiles(bbox BoundingBox) {
	log.Println("Generating fresh tile list for the US...")
	tileKeys := make(map[int64]struct{})
	for lat := bbox.MinLat; lat <= bbox.MaxLat; lat += bbox.Step {
		for lon := bbox.MinLon; lon <= bbox.MaxLon; lon += bbox.Step {
			tileKey := morton.Encode(lat, lon, 13)
			tileKeys[tileKey] = struct{}{}
		}
	}
	var allTiles []int64
	for key := range tileKeys {
		allTiles = append(allTiles, key)
	}
	data, err := json.Marshal(allTiles)
	if err != nil {
		log.Fatalf("Failed to marshal tiles: %v", err)
	}
	if err := ioutil.WriteFile(allTilesFile, data, 0644); err != nil {
		log.Fatalf("Failed to write tiles file: %v", err)
	}
	log.Printf("Generated and saved %d unique tiles to %s.", len(allTiles), allTilesFile)
}

func retryFailed() {
	tilesToProcess, err := getFailedTilesToProcess()
	if err != nil {
		log.Fatalf("Failed to get failed tiles to process: %v", err)
	}

	if len(tilesToProcess) == 0 {
		log.Println("No failed tiles to process.")
		return
	}

	log.Printf("Starting retry crawl for %d failed tiles.", len(tilesToProcess))
	performCrawl(tilesToProcess, failedTilesRetryFile)
	log.Println("Finished retrying failed tiles.")
}

func crawlUS(bbox BoundingBox) {
	tilesToProcess, err := getTilesToProcess(bbox)
	if err != nil {
		log.Fatalf("Failed to get tiles to process: %v", err)
	}

	if len(tilesToProcess) == 0 {
		log.Println("All tiles have been processed. Nothing to do.")
		return
	}

	log.Printf("Starting crawl for %d tiles.", len(tilesToProcess))
	performCrawl(tilesToProcess, failedTilesFile)
	log.Println("Finished crawling the US.")
}

func performCrawl(tilesToProcess []int64, failureFile string) {
	outputFile, err := os.OpenFile(outputDataFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open output file: %v", err)
	}
	defer outputFile.Close()

	processedFile, err := os.OpenFile(processedTilesFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open processed tiles file: %v", err)
	}
	defer processedFile.Close()

	failedFile, err := os.OpenFile(failureFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open failed tiles file: %v", err)
	}
	defer failedFile.Close()

	keyChan := make(chan int64, numWorkers)
	resultsChan := make(chan []lib.AP, numWorkers)
	processedChan := make(chan int64, numWorkers)
	failedChan := make(chan int64, numWorkers)
	var wg sync.WaitGroup

	// Graceful shutdown setup
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("Shutdown signal received. Finishing in-progress tasks...")
		// Future: could implement more graceful shutdown logic here if needed
		os.Exit(0)
	}()

	// Start writer goroutines
	var writerWg sync.WaitGroup
	writerWg.Add(3)
	go func() {
		defer writerWg.Done()
		for aps := range resultsChan {
			for _, ap := range aps {
				line := fmt.Sprintf("MAC: %s - %f %f\n", ap.BSSID, ap.Location.Lat, ap.Location.Long)
				if _, err := outputFile.WriteString(line); err != nil {
					log.Printf("Error writing to output file: %v\n", err)
				}
			}
		}
	}()
	go func() {
		defer writerWg.Done()
		for key := range processedChan {
			if _, err := processedFile.WriteString(fmt.Sprintf("%d\n", key)); err != nil {
				log.Printf("Error writing to processed file: %v\n", err)
			}
		}
	}()
	go func() {
		defer writerWg.Done()
		for key := range failedChan {
			if _, err := failedFile.WriteString(fmt.Sprintf("%d\n", key)); err != nil {
				log.Printf("Error writing to failed file: %v\n", err)
			}
		}
	}()

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range keyChan {
				aps, err := getTileWithRetry(key, maxRetries)
				if err != nil {
					log.Println(err) // Log final failure
					failedChan <- key
					continue
				}
				processedChan <- key
				if len(aps) > 0 {
					resultsChan <- aps
				}
			}
		}()
	}

	// Feed keys to workers
	for _, key := range tilesToProcess {
		keyChan <- key
	}
	close(keyChan)

	wg.Wait()
	close(resultsChan)
	close(processedChan)
	close(failedChan)
	writerWg.Wait()
}

func getFailedTilesToProcess() ([]int64, error) {
	var failedTiles []int64
	if _, err := os.Stat(failedTilesFile); os.IsNotExist(err) {
		return nil, nil // No failed tiles file, nothing to process
	}

	data, err := ioutil.ReadFile(failedTilesFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read failed tiles file: %w", err)
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		key, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			log.Printf("Warning: could not parse line in failed tiles file: %s", line)
			continue
		}
		failedTiles = append(failedTiles, key)
	}
	log.Printf("Loaded %d failed tiles.", len(failedTiles))

	processedTiles := make(map[int64]struct{})
	if _, err := os.Stat(processedTilesFile); err == nil {
		data, err := ioutil.ReadFile(processedTilesFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read processed tiles file: %w", err)
		}
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			key, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				log.Printf("Warning: could not parse line in processed tiles file: %s", line)
				continue
			}
			processedTiles[key] = struct{}{}
		}
		log.Printf("Loaded %d processed tiles to filter against.", len(processedTiles))
	}

	var tilesToProcess []int64
	for _, key := range failedTiles {
		if _, found := processedTiles[key]; !found {
			tilesToProcess = append(tilesToProcess, key)
		}
	}

	return tilesToProcess, nil
}

func getTilesToProcess(bbox BoundingBox) ([]int64, error) {
	var allTiles []int64
	if _, err := os.Stat(allTilesFile); os.IsNotExist(err) {
		log.Println("Tile list not found. Generating for the first time...")
		tileKeys := make(map[int64]struct{})
		for lat := bbox.MinLat; lat <= bbox.MaxLat; lat += bbox.Step {
			for lon := bbox.MinLon; lon <= bbox.MaxLon; lon += bbox.Step {
				tileKey := morton.Encode(lat, lon, 13)
				tileKeys[tileKey] = struct{}{}
			}
		}
		for key := range tileKeys {
			allTiles = append(allTiles, key)
		}
		data, err := json.Marshal(allTiles)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal tiles: %w", err)
		}
		if err := ioutil.WriteFile(allTilesFile, data, 0644); err != nil {
			return nil, fmt.Errorf("failed to write tiles file: %w", err)
		}
		log.Printf("Generated and saved %d unique tiles.", len(allTiles))
	} else {
		data, err := ioutil.ReadFile(allTilesFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read tiles file: %w", err)
		}
		if err := json.Unmarshal(data, &allTiles); err != nil {
			return nil, fmt.Errorf("failed to unmarshal tiles: %w", err)
		}
		log.Printf("Loaded %d tiles from %s.", len(allTiles), allTilesFile)
	}

	processedTiles := make(map[int64]struct{})
	if _, err := os.Stat(processedTilesFile); err == nil {
		data, err := ioutil.ReadFile(processedTilesFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read processed tiles file: %w", err)
		}
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			key, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				log.Printf("Warning: could not parse line in processed tiles file: %s", line)
				continue
			}
			processedTiles[key] = struct{}{}
		}
		log.Printf("Loaded %d processed tiles.", len(processedTiles))
	}

	var tilesToProcess []int64
	for _, key := range allTiles {
		if _, found := processedTiles[key]; !found {
			tilesToProcess = append(tilesToProcess, key)
		}
	}

	return tilesToProcess, nil
}

func getTileWithRetry(key int64, retries int) ([]lib.AP, error) {
	var err error
	var aps []lib.AP
	for attempt := 0; attempt < retries; attempt++ {
		aps, err = lib.GetTile(key)
		if err == nil {
			return aps, nil // Success
		}
		if err.Error() == "unexpected status code: 404" {
			return nil, nil // Not an error, just no data for this tile.
		}
		log.Printf("Error getting tile %d (attempt %d/%d): %v. Retrying...", key, attempt+1, retries, err)
		time.Sleep(time.Second * time.Duration(1<<attempt)) // Exponential backoff
	}
	return nil, fmt.Errorf("failed to get tile %d after %d attempts: %w", key, retries, err)
}
