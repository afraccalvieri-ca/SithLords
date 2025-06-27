# Confronto tra due implementazioni di ordinamento e merge di grandi file in Go

Questo repository contiene due versioni di un programma Go per ordinare e unire grandi file di testo suddivisi in chunk:

- **Versione Performante**: utilizza un merge parallelo a gruppi con buffering efficiente.
- **Versione Base**: esegue un merge sequenziale con uno scanner persistente per ogni chunk.

---

## Descrizione generale

Entrambe le versioni:

- Suddividono il file di input in chunk di dimensione limitata (max 100MB o 500.000 righe).
- Ordinano ogni chunk in parallelo usando un pool di worker.
- Scrivono i chunk ordinati su disco.
- Effettuano il merge finale dei chunk ordinati in un singolo file.

---

## Versione Base (meno performante)

- Il merge finale avviene sequenzialmente su tutti i chunk.
- Per ogni chunk viene mantenuto un `chunkReader` con un `bufio.Scanner` persistente.
- Il buffer di lettura è riempito con batch di righe (9000 per default).
- L’heap minimo gestisce i valori da tutti i chunk simultaneamente.
- Buffer di scrittura relativamente grande (16 MB).
- Utilizza un solo thread per il merge, quindi il merge è il collo di bottiglia.
- Maggiore utilizzo della memoria e tempi di attesa più lunghi per grandi quantità di chunk.

---

## Versione Performante (ottimizzata)

- Introduce un **merge parallelo a gruppi**: i chunk sono divisi in gruppi di 16 e ogni gruppo viene fuso in parallelo, generando file intermedi.
- Il merge finale è un concatenamento sequenziale dei file intermedi, molto più veloce perché i file intermedi sono già ordinati e non serve più l’heap.
- Mantiene un buffering efficiente in lettura e scrittura per minimizzare I/O e overhead.
- Miglior utilizzo delle CPU multiple, sfruttando il parallelismo nativo di Go.
- Risultati osservati:
  - Incremento del throughput di scrittura da 50 MB/s fino a picchi di 90 MB/s.
  - Riduzione del tempo totale di merge di circa 15 secondi.
  - Controllo più stabile dell’uso di RAM (sotto 500 MB durante merge).
- Assume che i chunk siano già ordinati internamente, consentendo merge più veloci.

---

## Confronto dettagliato

| Aspetto                  | Versione Base                         | Versione Performante              |
|--------------------------|------------------------------------|----------------------------------|
| **Parallelismo**         | Merge sequenziale                  | Merge parallelo a gruppi          |
| **Buffering lettura**    | Batch da 9000 righe                | Batch da 9000 righe               |
| **Buffering scrittura**  | 16 MB                             | 4 MB                             |
| **Gestione heap**        | Heap su tutti i chunk              | Heap su chunk di gruppo (16)      |
| **Uso RAM durante merge**| > 500 MB                         | < 500 MB                         |
| **Throughput scrittura** | ~50 MB/s                         | Picchi di 90 MB/s                |
| **Tempo totale merge**   | Maggiore                         | Ridotto (circa 3s in meno)       |
| **Scalabilità CPU**      | Limitata (merge sequenziale)      | Elevata (merge parallelo)         |

---

## Considerazioni

- Il miglioramento principale nella versione performante è il **merge parallelo a gruppi**, che riduce drasticamente il tempo di merge.
- Il buffering efficiente, unito a scritture meno pesanti, permette una gestione migliore della RAM e del I/O.
- La versione base rimane valida per dataset più piccoli o quando la semplicità è preferita.
- La versione performante è consigliata per dataset molto grandi e sistemi multi-core.

---

## Come usare

- Compilare con `go build` e eseguire.
- Assicurarsi che il file di input sia nel percorso specificato (`random_2gb_data` o altro).
- I chunk verranno scritti nella cartella `chunks`.
- L’output finale sarà prodotto nel percorso indicato (`E:/merged`).

---

Se hai domande o vuoi approfondire ulteriormente, fammi sapere!

---

*Creato con l’aiuto di ChatGPT*  
