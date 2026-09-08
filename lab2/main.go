package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <num_threads> <matrix_size>\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "Examples:")
	fmt.Fprintln(os.Stderr, "  lab2 2 1000")
	fmt.Fprintln(os.Stderr, "  lab2 4 1000")
	fmt.Fprintln(os.Stderr, "  lab2 8 1000")
	os.Exit(1)
}

// Последовательное умножение матриц
// Используется порядок циклов i-k-j для оптимальной работы с кэшем процессора.
func multiplySequential(A, B, C []float64, N int) {
	for i := 0; i < N; i++ {
		for k := 0; k < N; k++ {
			aik := A[i*N+k]
			for j := 0; j < N; j++ {
				C[i*N+j] += aik * B[k*N+j]
			}
		}
	}
}

// Параллельное умножение матриц
// Внешний цикл по i распараллеливается между потоками.
func multiplyParallel(A, B, C []float64, N, numThreads int) {
	var wg sync.WaitGroup
	
	// Вычисляем базовое количество строк на один поток
	rowsPerThread := N / numThreads

	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		
		startRow := t * rowsPerThread
		endRow := startRow + rowsPerThread
		
		// Последний поток забирает себе остаток, если N не делится нацело
		if t == numThreads-1 {
			endRow = N
		}

		go func(start, end int) {
			defer wg.Done()
			// Каждый поток работает только со своим диапазоном строк
			for i := start; i < end; i++ {
				for k := 0; k < N; k++ {
					aik := A[i*N+k]
					for j := 0; j < N; j++ {
						C[i*N+j] += aik * B[k*N+j]
					}
				}
			}
		}(startRow, endRow)
	}

	wg.Wait()
}

// Функция для подсчета контрольной суммы (чтобы доказать корректность)
func checksum(M []float64) float64 {
	var sum float64
	for _, v := range M {
		sum += v
	}
	return sum
}

func main() {
	if len(os.Args) < 3 {
		usage()
	}

	numThreads, err := strconv.Atoi(os.Args[1])
	if err != nil || numThreads <= 0 {
		fmt.Fprintf(os.Stderr, "Error: num_threads must be a positive integer\n")
		os.Exit(1)
	}

	N, err := strconv.Atoi(os.Args[2])
	if err != nil || N <= 0 {
		fmt.Fprintf(os.Stderr, "Error: matrix_size must be a positive integer\n")
		os.Exit(1)
	}

	// Ограничиваем количество потоков ОС, которые может использовать Go
	runtime.GOMAXPROCS(numThreads)

	fmt.Printf("Matrix size: %dx%d\n", N, N)
	fmt.Printf("Threads requested: %d\n", numThreads)
	fmt.Println("----------------------------------------")

	// Инициализация матриц
	// Используем 1D срезы для непрерывного расположения в памяти (как в C/C++)
	size := N * N
	A := make([]float64, size)
	B := make([]float64, size)
	C_seq := make([]float64, size)
	C_par := make([]float64, size)

	// Заполняем матрицы детерминированными псевдослучайными числами
	r := rand.New(rand.NewSource(42))
	for i := 0; i < size; i++ {
		A[i] = r.Float64() * 10.0
		B[i] = r.Float64() * 10.0
	}

	// 1. Последовательное выполнение
	fmt.Print("Running sequential multiplication... ")
	startSeq := time.Now()
	multiplySequential(A, B, C_seq, N)
	seqDuration := time.Since(startSeq)
	fmt.Printf("Done in %v\n", seqDuration)

	// 2. Параллельное выполнение
	fmt.Printf("Running parallel multiplication (%d threads)... ", numThreads)
	startPar := time.Now()
	multiplyParallel(A, B, C_par, N, numThreads)
	parDuration := time.Since(startPar)
	fmt.Printf("Done in %v\n", parDuration)

	// 3. Проверка корректности
	sumSeq := checksum(C_seq)
	sumPar := checksum(C_par)
	
	// Допускаем крошечную погрешность из-за особенностей float64
	match := math.Abs(sumSeq-sumPar) < 1e-3

	fmt.Println("----------------------------------------")
	fmt.Printf("Sequential Time: %v\n", seqDuration)
	fmt.Printf("Parallel Time:   %v\n", parDuration)
	
	if parDuration > 0 {
		speedup := float64(seqDuration) / float64(parDuration)
		fmt.Printf("Speedup:         %.2fx\n", speedup)
	}
	
	fmt.Printf("Checksum Seq:    %.2f\n", sumSeq)
	fmt.Printf("Checksum Par:    %.2f\n", sumPar)
	fmt.Printf("Results Match:   %t\n", match)

	if !match {
		fmt.Println("ERROR: Parallel results do not match sequential!")
		os.Exit(2)
	}
}