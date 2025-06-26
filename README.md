# Ordinamento Esterno (External Merge Sort) in Go

![Go Version](https://img.shields.io/badge/Go-1.18%2B-blue?style=for-the-badge&logo=go)

Questo repository contiene uno script in Go ad alte prestazioni per ordinare file di testo di grandi dimensioni (più grandi della RAM disponibile) utilizzando l'algoritmo **External Merge Sort**. Lo script è progettato per essere efficiente sia in termini di CPU che di utilizzo della memoria, sfruttando la concorrenza e una gestione attenta dell'I/O.

---

### Caratteristiche Principali

* **Elaborazione Parallela**: Sfrutta tutti i core della CPU disponibili (`runtime.NumCPU()`) per la fase iniziale di divisione e ordinamento dei chunk, riducendo significativamente i tempi di elaborazione.
* **Efficienza di Memoria**: Utilizza una quantità di RAM molto contenuta durante la fase di fusione (merge) grazie a buffer di lettura ottimizzati, permettendo di processare file di decine o centinaia di GB su macchine con memoria limitata.
* **I/O Ottimizzato**: Fa uso di buffer (`bufio.Reader` e `bufio.Writer`) per minimizzare il numero di chiamate di sistema e massimizzare il throughput del disco.
* **Configurabile**: Le performance possono essere ottimizzate per hardware specifico modificando le costanti definite all'inizio del file (dimensione dei chunk, dimensione dei buffer, etc.).
* **Deployment Semplice**: Compila in un **singolo binario statico** senza dipendenze esterne, rendendo il deployment estremamente semplice.

---

### Come Funziona

L'algoritmo è diviso in due macro-fasi:

1.  **Fase 1: Divisione e Ordinamento (Split & Sort)**
    * Il file di input originale viene letto sequenzialmente.
    * I dati vengono raggruppati in "chunk" di dimensioni gestibili (es. 100 MB).
    * Ogni chunk viene distribuito a un worker (goroutine) che lo ordina in memoria usando l'ordinamento standard di Go.
    * I chunk, ora ordinati internamente, vengono salvati su disco come file temporanei.

2.  **Fase 2: Fusione Ordinata (K-Way Merge)**
    * Il programma apre tutti i file chunk ordinati.
    * Utilizza una struttura dati **Min-Heap** per tenere traccia della riga successiva (alfabeticamente più piccola) tra tutti i chunk.
    * In un ciclo, estrae la riga più piccola dall'heap, la scrive nel file di output finale e la rimpiazza nell'heap con la riga successiva proveniente dallo stesso chunk.
    * Questo processo continua finché tutte le righe di tutti i chunk non sono state fuse nel file di output, che risulterà globalmente ordinato.

---

### Prerequisiti

* È necessaria un'installazione funzionante di **Go** (versione 1.18 o successiva è consigliata).

---

### Utilizzo

1.  **Clonare il repository o salvare il file**
    Salvare il codice come `main.go` in una directory a scelta.

2.  **Preparare un file di dati (Opzionale)**
    Incollare il file `random_2gb_data` nella stessa cartella di main.go
    ```
    *Nota: `main.go` si aspetta righe di 32 caratteri. Per un test più realistico, il file dovrebbe contenere righe di testo separate da newline.*

3.  **Eseguire lo script**
    Aprire un terminale nella directory contenente `main.go` ed eseguire:
    ```bash
    go run main.go
    ```
    Lo script creerà una cartella per i chunk (es. `chunks`) e il file di output finale (`merged.txt`).

4.  **Compilazione per la Produzione (Consigliato)**
    Per ottenere le massime performance, è consigliabile compilare il programma in un binario nativo:
    ```bash
    go build -o external-sorter main.go
    ```
    E poi eseguirlo:
    ```bash
    ./external-sorter
    ```

---

### Configurazione

Le performance possono essere ottimizzate modificando le costanti all'inizio del file `main.go`:

| Costante         | Descrizione                                                                            | Impatto                                                                    |
| :--------------- | :------------------------------------------------------------------------------------- | :------------------------------------------------------------------------- |
| `maxDiskSize`    | Dimensione massima in byte di un chunk prima che venga ordinato e scritto su disco.      | Un valore più alto usa più RAM nella Fase 1 ma crea meno chunk.            |
| `maxItems`       | Numero massimo di righe per un chunk. Un limite alternativo a `maxDiskSize`.             | Simile a `maxDiskSize`.                                                    |
| `bufferLines`    | Numero di righe lette in batch da ogni chunk durante la Fase 2 (Merge).                  | Valori più bassi riducono la RAM usata nel merge a costo di più letture da disco. |
| `readerBufSize`  | Dimensione del buffer di I/O per la lettura di ogni file chunk.                          | Simile a `bufferLines`.                                                    |
| `writerBufferSize`| Dimensione del buffer di scrittura per il file di output finale.                         | Un valore più grande è generalmente migliore per l'I/O.                    |

---

### Licenza

Rilasciato sotto la [Licenza MIT](https://opensource.org/licenses/MIT).
