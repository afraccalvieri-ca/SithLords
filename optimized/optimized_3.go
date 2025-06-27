package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

// heapItem rappresenta un elemento nel heap usato per il merge.
type heapItem struct {
	value string
	index int
}

type minHeapBuffered []heapItem

func (h minHeapBuffered) Len() int            { return len(h) }
func (h minHeapBuffered) Less(i, j int) bool  { return h[i].value < h[j].value }
func (h minHeapBuffered) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeapBuffered) Push(x interface{}) { *h = append(*h, x.(heapItem)) }
func (h *minHeapBuffered) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type chunkReader struct {
	file    *os.File
	scanner *bufio.Scanner
	buffer  []string
	index   int
}

const (
	maxDiskSize      = 100 * 1024 * 1024
	maxItems         = 500_000
	strLength        = 32
	bufferLines      = 9000
	readerBufSize    = 256 * 1024
	writerBufferSize = 4 * 1024 * 1024
)

func main() {
	inputPath := "../random_2gb_data"
	outputDir := "chunks"
	outputFile := "E:/merged"

	start := time.Now()
	os.MkdirAll(outputDir, 0755)

	fmt.Println("🔹 Step 1: Split e ordinamento dei chunk...")
	if err := splitAndSortChunksParallel(inputPath, outputDir); err != nil {
		panic(err)
	}
	fmt.Println("✅ Split completato.")

	fmt.Println("🔹 Step 2: Merge finale parallelo...")
	if err := mergeChunksParallelGrouped(outputDir, outputFile); err != nil {
		panic(err)
	}
	fmt.Printf("✅ Merge completato in %s\n", time.Since(start))
}

func splitAndSortChunksParallel(inputFile, outputDir string) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	chunkSize := 0
	chunk := make([]string, 0, 100_000)
	chunkCount := 0
	chunkChan := make(chan struct {
		lines []string
		id    int
	}, 8)

	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range chunkChan {
				sort.Strings(job.lines)
				chunkPath := filepath.Join(outputDir, fmt.Sprintf("chunk_%03d.txt", job.id))
				f, err := os.Create(chunkPath)
				if err != nil {
					fmt.Fprintln(os.Stderr, "Errore creazione file chunk:", err)
					continue
				}
				writer := bufio.NewWriter(f)
				for _, s := range job.lines {
					writer.WriteString(s + "\n")
				}
				writer.Flush()
				f.Close()
			}
		}()
	}

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return err
		}

		if len(line) > 0 {
			clean := bytes.TrimSpace(line)
			if len(clean) == strLength {
				chunk = append(chunk, string(clean))
				chunkSize += len(clean) + 1
			}
		}

		if chunkSize >= maxDiskSize || len(chunk) >= maxItems || (err == io.EOF && len(chunk) > 0) {
			job := struct {
				lines []string
				id    int
			}{lines: append([]string(nil), chunk...), id: chunkCount}
			chunkChan <- job
			chunkCount++
			chunk = chunk[:0]
			chunkSize = 0
		}
		if err == io.EOF {
			break
		}
	}
	close(chunkChan)
	wg.Wait()
	return nil
}

func fillBuffer(r *chunkReader, count int) error {
	r.buffer = r.buffer[:0]
	for len(r.buffer) < count && r.scanner.Scan() {
		r.buffer = append(r.buffer, string(r.scanner.Bytes()))
	}
	return r.scanner.Err()
}

func mergeChunks(chunkFiles []string, outputFile string) error {
	readers := make([]*chunkReader, len(chunkFiles))
	for i, file := range chunkFiles {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(bufio.NewReaderSize(f, readerBufSize))
		r := &chunkReader{file: f, scanner: scanner, buffer: []string{}, index: i}
		if err := fillBuffer(r, bufferLines); err != nil {
			return err
		}
		readers[i] = r
	}
	defer func() {
		for _, r := range readers {
			r.file.Close()
		}
	}()

	h := &minHeapBuffered{}
	heap.Init(h)
	for _, r := range readers {
		if len(r.buffer) > 0 {
			heap.Push(h, heapItem{value: r.buffer[0], index: r.index})
			r.buffer = r.buffer[1:]
		}
	}

	out, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer out.Close()
	writer := bufio.NewWriterSize(out, writerBufferSize)

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		writer.WriteString(item.value + "\n")
		r := readers[item.index]
		if len(r.buffer) == 0 {
			_ = fillBuffer(r, bufferLines)
		}
		if len(r.buffer) > 0 {
			heap.Push(h, heapItem{value: r.buffer[0], index: r.index})
			r.buffer = r.buffer[1:]
		}
	}
	return writer.Flush()
}

func mergeChunksParallelGrouped(chunkDir, finalOutput string) error {
	files, err := filepath.Glob(filepath.Join(chunkDir, "chunk_*.txt"))
	if err != nil {
		return err
	}

	const groupSize = 16
	numGroups := (len(files) + groupSize - 1) / groupSize
	tempFiles := make([]string, numGroups)

	var wg sync.WaitGroup
	errChan := make(chan error, numGroups)

	for i := 0; i < numGroups; i++ {
		start := i * groupSize
		end := start + groupSize
		if end > len(files) {
			end = len(files)
		}
		group := files[start:end]
		partName := fmt.Sprintf("part_%02d", i)
		tempFiles[i] = partName

		wg.Add(1)
		go func(groupFiles []string, output string) {
			defer wg.Done()
			if err := mergeChunks(groupFiles, output); err != nil {
				errChan <- err
			}
		}(group, partName)
	}

	wg.Wait()
	close(errChan)
	if len(errChan) > 0 {
		return <-errChan
	}

	out, err := os.Create(finalOutput)
	if err != nil {
		return err
	}
	defer out.Close()
	writer := bufio.NewWriterSize(out, writerBufferSize)

	for _, part := range tempFiles {
		in, err := os.Open(part)
		if err != nil {
			return err
		}
		_, err = io.Copy(writer, in)
		in.Close()
		if err != nil {
			return err
		}
		os.Remove(part)
	}
	return writer.Flush()
}
