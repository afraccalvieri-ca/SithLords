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
// Contiene la stringa (value) e l'indice del chunkReader da cui proviene.
// Serve per mantenere traccia da quale file leggere la prossima riga.
type heapItem struct {
	value string // valore testuale della riga
	index int    // indice del chunkReader di origine
}

// minHeapBuffered è un heap minimo di heapItem ordinato alfabeticamente
// per mantenere sempre in cima la stringa più piccola.
type minHeapBuffered []heapItem

// Len restituisce la lunghezza dell'heap (numero di elementi)
func (h minHeapBuffered) Len() int { return len(h) }

// Less confronta due elementi dell'heap per mantenere ordine alfabetico
func (h minHeapBuffered) Less(i, j int) bool { return h[i].value < h[j].value }

// Swap scambia due elementi nell'heap
func (h minHeapBuffered) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push aggiunge un nuovo elemento all'heap (richiesto da container/heap)
func (h *minHeapBuffered) Push(x interface{}) {
	*h = append(*h, x.(heapItem))
}

// Pop rimuove e restituisce l'ultimo elemento dell'heap (richiesto da container/heap)
func (h *minHeapBuffered) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// chunkReader rappresenta un file chunk con un buffer interno.
// **MODIFICA CHIAVE**: Ora contiene un `*bufio.Scanner` per mantenere lo stato di lettura.
type chunkReader struct {
	file    *os.File       // file chunk aperto
	scanner *bufio.Scanner // Scanner per leggere il file in modo stateful
	buffer  []string       // buffer interno di righe lette in RAM
	index   int            // indice del chunkReader (per identificazione)
}

// Costanti per configurare dimensioni RAM e I/O buffer
const (
	maxDiskSize      = 100 * 1024 * 1024 // 100 MB massimo chunk su disco (al suo aumentare, aumenta la RAM in uso, fino al raggiungimento di maxItems)
	maxItems         = 500_000           // max elementi in memoria per chunk (al suo aumentare, aumenta la RAM in uso fino al raggiungimento di maxDiskSize)
	strLength        = 32                // lunghezza stringhe alfanumeriche
	bufferLines      = 9000              // numero di righe lette per batch da ogni chunk nel merge
	readerBufSize    = 512 * 1024        // buffer di lettura da 512 KB
	writerBufferSize = 16 * 1024 * 1024  // buffer di scrittura da 16 MB (da 32 si notano rallentamenti)
)

func main() {
	inputPath := "random_2gb_data" // file di input da ordinare
	outputDir := "chunks"          // cartella in cui scrivere i chunk ordinati
	outputFile := "E:/merged"      // file di output con il merge finale ordinato

	start := time.Now()
	os.MkdirAll(outputDir, 0755) // crea la directory di output, se non esiste

	fmt.Println("🔹 Step 1: Split e ordinamento dei chunk...")
	if err := splitAndSortChunksParallel(inputPath, outputDir); err != nil {
		panic(err)
	}
	fmt.Println("✅ Split completato.")

	fmt.Println("🔹 Step 2: Merge finale dei chunk...")
	if err := mergeChunks(outputDir, outputFile); err != nil {
		panic(err)
	}
	fmt.Printf("✅ Merge completato in %s\n", time.Since(start))
}

// splitAndSortChunksParallel legge chunk dal file input, li invia tramite canale a un pool di worker
// che ordinano e scrivono i chunk in parallelo migliorando l'uso delle CPU multiple.
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

	// Canale buffered per inviare chunk da ordinare ai worker
	chunkChan := make(chan struct {
		lines []string
		id    int
	}, 8)

	// Numero di worker = numero di CPU disponibili
	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup

	// Avvia i worker che ricevono chunk dal canale, li ordinano e scrivono su disco
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

	// Legge linee dal file, crea chunk e li invia ai worker tramite canale
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
			// Copia difensiva della slice prima di inviare ai worker
			job := struct {
				lines []string
				id    int
			}{
				lines: append([]string(nil), chunk...),
				id:    chunkCount,
			}
			chunkChan <- job

			chunkCount++
			chunk = chunk[:0]
			chunkSize = 0
		}

		if err == io.EOF {
			break
		}
	}

	close(chunkChan) // chiude il canale per terminare i worker
	wg.Wait()        // aspetta che tutti i worker finiscano
	return nil
}

// fillBuffer (CORRETTO) ora usa lo scanner persistente del chunkReader.
// Questo previene la perdita di dati che avveniva creando un nuovo scanner ad ogni chiamata.
func fillBuffer(r *chunkReader, count int) error {
	r.buffer = r.buffer[:0]
	for len(r.buffer) < count && r.scanner.Scan() {
		// MODIFICA CHIAVE rispetto a r.buffer = append(r.buffer, r.scanner.Text())
		// string(r.scanner.Bytes()) crea una nuova stringa, copiando i dati
		// in una nuova area di memoria. Questo previene il bug della sovrascrittura
		// e rende il codice robusto a prescindere dalla dimensione dei buffer.
		r.buffer = append(r.buffer, string(r.scanner.Bytes()))
	}
	return r.scanner.Err()
}

// mergeChunks effettua il merge finale ordinato di tutti i chunk.
// Usa un heap minimo per mantenere in cima la stringa alfabeticamente più piccola,
// legge in batch da ciascun file per efficienza e scrive su outputFile.
func mergeChunks(chunkDir string, outputFile string) error {
	files, err := filepath.Glob(filepath.Join(chunkDir, "chunk_*.txt"))
	if err != nil {
		return err
	}

	// Apre tutti i file chunk e crea un chunkReader per ciascuno
	readers := make([]*chunkReader, len(files))
	for i, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return err
		}

		// **MODIFICA CHIAVE**: Inizializza lo scanner una sola volta per file
		// e lo assegna al chunkReader. Questo preserva lo stato di lettura.
		scanner := bufio.NewScanner(bufio.NewReaderSize(f, readerBufSize))
		r := &chunkReader{
			file:    f,
			scanner: scanner,
			buffer:  []string{},
			index:   i,
		}

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

	// Inizializza l'heap minimo e inserisce la prima riga di ogni chunk nel heap
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

	// Ciclo principale: estrae l'elemento più piccolo dall'heap, lo scrive,
	// e lo rimpiazza con la riga successiva dello stesso chunkReader.
	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem) // Estrae l'elemento più piccolo
		writer.WriteString(item.value + "\n")

		r := readers[item.index]

		// Se il buffer in RAM del reader è vuoto, prova a riempirlo dal file.
		if len(r.buffer) == 0 {
			if err := fillBuffer(r, bufferLines); err != nil {
				// Non è un errore fatale, potrebbe essere solo EOF
			}
		}

		// Se dopo il tentativo di riempimento il buffer ha ancora dati,
		// inserisce la prossima riga nell'heap.
		if len(r.buffer) > 0 {
			heap.Push(h, heapItem{value: r.buffer[0], index: r.index})
			r.buffer = r.buffer[1:]
		}
	}
	return writer.Flush()
}
