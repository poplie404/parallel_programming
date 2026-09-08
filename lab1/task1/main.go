package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
)

func main() {
	// Ожидаем как минимум один аргумент — число потоков.
	// Для единообразия с другими лабораторными также поддерживается второй
	// аргумент — размер задачи. В Задаче 1 он не используется.
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <num_threads> [task_size]\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "Examples:")
		fmt.Fprintln(os.Stderr, "  lab1 8")
		fmt.Fprintln(os.Stderr, "  lab1 8 1")
		os.Exit(1)
	}

	numThreads, err := strconv.Atoi(os.Args[1])
	if err != nil || numThreads <= 0 {
		fmt.Fprintf(os.Stderr, "Error: num_threads must be a positive integer, got %q\n", os.Args[1])
		os.Exit(1)
	}

	// Если передан размер задачи, проверяем его, но в Задаче 1 не используем.
	if len(os.Args) > 2 {
		taskSize, err := strconv.Atoi(os.Args[2])
		if err != nil || taskSize <= 0 {
			fmt.Fprintf(os.Stderr, "Error: task_size must be a positive integer, got %q\n", os.Args[2])
			os.Exit(1)
		}
		_ = taskSize
	}

	// Устанавливаем максимальное количество потоков процессора,
	// которые может использовать Go-программа.
	runtime.GOMAXPROCS(numThreads)

	var wg sync.WaitGroup
	var printMu sync.Mutex

	// Создаем numThreads горутин.
	// В рамках данной лабораторной они рассматриваются как аналог потоков.
	for i := 1; i <= numThreads; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			// Мьютекс защищает вывод, чтобы строки разных горутин
			// не перемешивались внутри одной строки.
			printMu.Lock()
			fmt.Printf("Thread %d of %d: Hello World\n", id, numThreads)
			printMu.Unlock()
		}(i)
	}

	// Аналог завершения параллельной области:
	// главная горутина ждет завершения всех остальных.
	wg.Wait()
}
