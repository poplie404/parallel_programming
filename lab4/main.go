package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "  %s gen <task_size> [file]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <threads> <task_size> [file]\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Examples:")
	fmt.Fprintln(os.Stderr, "  lab4 gen 2000000 points.txt")
	fmt.Fprintln(os.Stderr, "  lab4 8 2000000 points.txt")
	os.Exit(1)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}

// generatePoints создаёт файл с точками (i, 2*i, 3*i)
func generatePoints(filename string, n int) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	for i := 0; i < n; i++ {
		if _, err := fmt.Fprintf(w, "%d %d %d\n", i, 2*i, 3*i); err != nil {
			return err
		}
	}
	return w.Flush()
}

// readPoints читает первые need точек из файла
func readPoints(filename string, need int) ([]float64, []float64, []float64, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, nil, nil, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)

	xs := make([]float64, need)
	ys := make([]float64, need)
	zs := make([]float64, need)

	count := 0
	lineNo := 0

	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 3 {
			return nil, nil, nil, fmt.Errorf("line %d: expected 3 coordinates, got %d", lineNo, len(fields))
		}

		x, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("line %d: bad x: %v", lineNo, err)
		}
		y, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("line %d: bad y: %v", lineNo, err)
		}
		z, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("line %d: bad z: %v", lineNo, err)
		}

		xs[count] = x
		ys[count] = y
		zs[count] = z
		count++

		if count == need {
			break
		}
	}

	if err := sc.Err(); err != nil {
		return nil, nil, nil, err
	}

	if count < need {
		return nil, nil, nil, fmt.Errorf(
			"file %s contains only %d points, but task_size=%d; regenerate or use a larger file",
			filename, count, need,
		)
	}

	return xs, ys, zs, nil
}

// sumSlice — последовательная сумма элементов среза
func sumSlice(vals []float64) float64 {
	var s float64
	for _, v := range vals {
		s += v
	}
	return s
}

// computeWithCriticalSections — функциональная декомпозиция с глобальной суммой
// и критическими секциями.
//
// Три горутины (sumX, sumY, sumZ) каждая вычисляет сумму своей координаты,
// а затем добавляет её в общую глобальную сумму через критическую секцию
// (мьютекс). Это аналог конструкции:
//
//	#pragma omp parallel sections
//	{
//	    #pragma omp section
//	    { double sx = calc_sum_x();
//	      #pragma omp critical
//	      global_sum += sx; }
//	    ...
//	}
func computeWithCriticalSections(xs, ys, zs []float64) float64 {
	var globalSum float64
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(3)

	// Функция 1: сумма по x, добавление в глобальную сумму через критическую секцию
	go func() {
		defer wg.Done()
		sx := sumSlice(xs)
		mu.Lock() // критическая секция
		globalSum += sx
		mu.Unlock()
	}()

	// Функция 2: сумма по y
	go func() {
		defer wg.Done()
		sy := sumSlice(ys)
		mu.Lock() // критическая секция
		globalSum += sy
		mu.Unlock()
	}()

	// Функция 3: сумма по z
	go func() {
		defer wg.Done()
		sz := sumSlice(zs)
		mu.Lock() // критическая секция
		globalSum += sz
		mu.Unlock()
	}()

	wg.Wait()
	return globalSum
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	// Режим генерации входного файла
	if os.Args[1] == "gen" {
		if len(os.Args) < 3 {
			usage()
		}
		n, err := strconv.Atoi(os.Args[2])
		if err != nil || n <= 0 {
			fatal(fmt.Errorf("task_size must be a positive integer"))
		}
		filename := "points.txt"
		if len(os.Args) > 3 {
			filename = os.Args[3]
		}
		start := time.Now()
		if err := generatePoints(filename, n); err != nil {
			fatal(err)
		}
		fmt.Printf("Generated %d points in file %s\n", n, filename)
		fmt.Printf("Generation time: %s\n", time.Since(start))
		return
	}

	// Основной режим: <threads> <task_size> [file]
	if len(os.Args) < 3 {
		usage()
	}

	threads, err := strconv.Atoi(os.Args[1])
	if err != nil || threads <= 0 {
		fatal(fmt.Errorf("threads must be a positive integer"))
	}

	taskSize, err := strconv.Atoi(os.Args[2])
	if err != nil || taskSize <= 0 {
		fatal(fmt.Errorf("task_size must be a positive integer"))
	}

	filename := "points.txt"
	if len(os.Args) > 3 {
		filename = os.Args[3]
	}

	runtime.GOMAXPROCS(threads)

	// Чтение точек из файла
	readStart := time.Now()
	xs, ys, zs, err := readPoints(filename, taskSize)
	if err != nil {
		fatal(err)
	}
	readDuration := time.Since(readStart)

	n := taskSize
	nf := float64(n)

	fmt.Println("=== Lab 4: (sum_x + sum_y + sum_z) / (3*N) ===")
	fmt.Println("Version: functional decomposition with global sum and critical sections")
	fmt.Printf("File: %s\n", filename)
	fmt.Printf("Threads (GOMAXPROCS): %d\n", threads)
	fmt.Printf("Task size: %d points\n", n)
	fmt.Printf("Read time: %s\n", readDuration)
	fmt.Println("Functions: 3 (sumX, sumY, sumZ)")
	fmt.Println("Global sum protected by mutex (critical section)")
	fmt.Println()

	// Вычисление с критическими секциями
	computeStart := time.Now()
	globalSum := computeWithCriticalSections(xs, ys, zs)
	computeDuration := time.Since(computeStart)

	result := globalSum / (3.0 * nf)

	fmt.Printf("Compute time: %s\n", computeDuration)
	fmt.Printf("Total time: %s\n", readDuration+computeDuration)
	fmt.Printf("Global sum: %.6f\n", globalSum)
	fmt.Printf("Result (sum / (3*N)): %.6f\n", result)
}
