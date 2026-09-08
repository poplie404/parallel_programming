package main

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type workerStat struct {
	iterations int
	blocks     int
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <num_threads> <task_size> [schedule] [chunk]\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "schedule: static | dynamic | guided | runtime (default: static)")
	fmt.Fprintln(os.Stderr, "chunk: positive integer or 0 for default behavior")
	fmt.Fprintln(os.Stderr, "Examples:")
	fmt.Fprintln(os.Stderr, "  task2 8 16000 static")
	fmt.Fprintln(os.Stderr, "  task2 8 16000 dynamic 100")
	fmt.Fprintln(os.Stderr, "  task2 8 16000 guided 100")
	fmt.Fprintln(os.Stderr, "  TASK2_SCHEDULE=dynamic,100 task2 8 16000 runtime")
	os.Exit(1)
}

func parseRuntimeEnv(defaultChunk int) (string, int) {
	raw := os.Getenv("task2_SCHEDULE")
	if raw == "" {
		raw = os.Getenv("OMP_SCHEDULE")
	}

	if raw == "" {
		return "static", defaultChunk
	}

	parts := strings.Split(raw, ",")
	schedule := strings.ToLower(strings.TrimSpace(parts[0]))
	chunk := defaultChunk

	if len(parts) > 1 {
		if c, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil && c > 0 {
			chunk = c
		}
	}

	switch schedule {
	case "static", "dynamic", "guided":
		return schedule, chunk
	default:
		return "static", defaultChunk
	}
}

func main() {
	if len(os.Args) < 3 {
		usage()
	}

	threads, err := strconv.Atoi(os.Args[1])
	if err != nil || threads <= 0 {
		fmt.Fprintf(os.Stderr, "Error: num_threads must be a positive integer, got %q\n", os.Args[1])
		os.Exit(1)
	}

	n, err := strconv.Atoi(os.Args[2])
	if err != nil || n <= 0 {
		fmt.Fprintf(os.Stderr, "Error: task_size must be a positive integer, got %q\n", os.Args[2])
		os.Exit(1)
	}

	requestedSchedule := "static"
	if len(os.Args) > 3 {
		requestedSchedule = strings.ToLower(os.Args[3])
	}

	chunk := 0
	if len(os.Args) > 4 {
		chunk, err = strconv.Atoi(os.Args[4])
		if err != nil || chunk < 0 {
			fmt.Fprintf(os.Stderr, "Error: chunk must be zero or a positive integer, got %q\n", os.Args[4])
			os.Exit(1)
		}
	}

	switch requestedSchedule {
	case "static", "dynamic", "guided", "runtime":
	default:
		fmt.Fprintf(os.Stderr, "Error: unsupported schedule %q\n", requestedSchedule)
		usage()
	}

	resolvedSchedule := requestedSchedule
	resolvedChunk := chunk

	if requestedSchedule == "runtime" {
		resolvedSchedule, resolvedChunk = parseRuntimeEnv(chunk)
	}

	displayChunk := resolvedChunk
	if resolvedSchedule == "dynamic" && displayChunk <= 0 {
		displayChunk = 1
	}
	if resolvedSchedule == "guided" && displayChunk <= 0 {
		displayChunk = 1
	}

	runtime.GOMAXPROCS(threads)

	// Исходный массив.
	// Значение элемента массива равно его порядковому номеру.
	// Используется нумерация с 0, то есть a[0] = 0, a[1] = 1, ...
	a := make([]float64, n)
	for i := range a {
		a[i] = float64(i)
	}

	// Результирующий массив.
	// Крайние элементы в условии не определены.
	// В данной реализации они копируются из исходного массива:
	// b[0] = a[0], b[n-1] = a[n-1].
	b := make([]float64, n)

	switch {
	case n == 1:
		b[0] = a[0]
	case n == 2:
		b[0] = a[0]
		b[1] = a[1]
	default:
		b[0] = a[0]
		b[n-1] = a[n-1]
	}

	internal := 0
	if n >= 3 {
		internal = n - 2
	}

	stats := make([]workerStat, threads)

	processBlock := func(start, end int) {
		for i := start; i <= end; i++ {
			b[i] = (a[i-1] + a[i] + a[i+1]) / 3.0
		}
	}

	started := time.Now()

	if internal > 0 {
		var wg sync.WaitGroup

		// Мьютекс используется только для динамического и guided-планирования.
		// Для статического распределения общая очередь не нужна.
		var schedMu sync.Mutex

		next := 1
		endIdx := n - 2

		for id := 0; id < threads; id++ {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				// Привязка горутины к потоку ОС, чтобы ближе соответствовать
				// терминологии "поток/процесс" из задания.
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()

				switch resolvedSchedule {
				case "static":
					if resolvedChunk <= 0 {
						// static без chunk: распределяем итерации непрерывными блоками.
						total := internal
						base := total / threads
						rem := total % threads

						size := base
						if id < rem {
							size++
						}

						startIdx := 1 + id*base + minInt(id, rem)

						if size > 0 {
							blockEnd := startIdx + size - 1
							processBlock(startIdx, blockEnd)

							stats[id].iterations += size
							stats[id].blocks++
						}
					} else {
						// static с chunk: блоки фиксированного размера раздаются циклически.
						blockIdx := id

						for {
							startIdx := 1 + blockIdx*resolvedChunk
							if startIdx > endIdx {
								break
							}

							blockEnd := minInt(startIdx+resolvedChunk-1, endIdx)

							processBlock(startIdx, blockEnd)

							stats[id].iterations += blockEnd - startIdx + 1
							stats[id].blocks++

							blockIdx += threads
						}
					}

				case "dynamic":
					blockSize := resolvedChunk
					if blockSize <= 0 {
						blockSize = 1
					}

					for {
						schedMu.Lock()

						if next > endIdx {
							schedMu.Unlock()
							break
						}

						startIdx := next
						blockEnd := startIdx + blockSize - 1
						if blockEnd > endIdx {
							blockEnd = endIdx
						}

						next = blockEnd + 1

						schedMu.Unlock()

						processBlock(startIdx, blockEnd)

						stats[id].iterations += blockEnd - startIdx + 1
						stats[id].blocks++
					}

				case "guided":
					minChunk := resolvedChunk
					if minChunk <= 0 {
						minChunk = 1
					}

					for {
						schedMu.Lock()

						if next > endIdx {
							schedMu.Unlock()
							break
						}

						remaining := endIdx - next + 1

						// Эмуляция guided-распределения:
						// размер очередного блока уменьшается по мере обработки массива,
						// но не становится меньше минимального блока.
						blockSize := remaining / (2 * threads)
						if blockSize < minChunk {
							blockSize = minChunk
						}
						if blockSize > remaining {
							blockSize = remaining
						}

						startIdx := next
						blockEnd := startIdx + blockSize - 1
						if blockEnd > endIdx {
							blockEnd = endIdx
						}

						next = blockEnd + 1

						schedMu.Unlock()

						processBlock(startIdx, blockEnd)

						stats[id].iterations += blockEnd - startIdx + 1
						stats[id].blocks++
					}
				}
			}(id)
		}

		wg.Wait()
	}

	elapsed := time.Since(started)

	// Контрольная сумма считается последовательно после завершения всех потоков,
	// чтобы избежать гонок при подсчете.
	var checksum float64
	for _, v := range b {
		checksum += v
	}

	// Так как a[i] = i, для внутренних элементов:
	// b[i] = (i - 1 + i + i + 1) / 3 = i.
	// Крайние элементы скопированы из a.
	// Поэтому ожидается сумма 0 + 1 + ... + (n - 1).
	expected := float64(n-1) * float64(n) / 2.0
	tolerance := math.Max(0.5, 1e-9*math.Abs(expected))
	ok := math.Abs(checksum-expected) <= tolerance

	fmt.Printf("Threads: %d\n", threads)
	fmt.Printf("Array size: %d\n", n)
	fmt.Printf("Internal elements: %d\n", internal)
	fmt.Printf("Requested schedule: %s\n", requestedSchedule)

	if requestedSchedule == "runtime" {
		fmt.Printf("Resolved schedule: %s\n", resolvedSchedule)
	}

	if resolvedSchedule == "static" && resolvedChunk <= 0 {
		fmt.Println("Chunk: 0 (static contiguous distribution)")
	} else {
		fmt.Printf("Chunk/min chunk: %d\n", displayChunk)
	}

	fmt.Printf("Compute time: %s\n", elapsed)

	totalProcessed := 0
	totalBlocks := 0

	for id := 0; id < threads; id++ {
		fmt.Printf("Worker %2d: iterations=%6d, blocks=%6d\n",
			id, stats[id].iterations, stats[id].blocks)

		totalProcessed += stats[id].iterations
		totalBlocks += stats[id].blocks
	}

	fmt.Printf("Total processed: %d of %d, total blocks: %d\n",
		totalProcessed, internal, totalBlocks)

	if n <= 20 {
		fmt.Print("b: ")
		for i := 0; i < n; i++ {
			fmt.Printf("%d:%.0f ", i, b[i])
		}
		fmt.Println()
	} else {
		fmt.Print("First 10 b: ")
		for i := 0; i < 10; i++ {
			fmt.Printf("%d:%.0f ", i, b[i])
		}
		fmt.Println()

		fmt.Print("Last 10 b: ")
		for i := n - 10; i < n; i++ {
			fmt.Printf("%d:%.0f ", i, b[i])
		}
		fmt.Println()
	}

	fmt.Printf("Checksum: %.0f, expected: %.0f, OK: %t\n", checksum, expected, ok)

	if !ok {
		os.Exit(2)
	}
}
