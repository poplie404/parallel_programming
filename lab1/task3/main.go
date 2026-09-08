package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <num_threads> <task_size> [method]\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "methods: all, mutex, cond, chan_chain, chan_array, wg_chain")
	fmt.Fprintln(os.Stderr, "Examples:")
	fmt.Fprintln(os.Stderr, "  task3 8 1000")
	fmt.Fprintln(os.Stderr, "  task3 8 1000 cond")
	os.Exit(1)
}

// Вспомогательная функция для имитации полезной нагрузки (использует taskSize)
func doWork(taskSize int) {
	for k := 0; k < taskSize; k++ {
		_ = k
	}
}

// ==========================================
// СПОСОБ 1: Активное ожидание (Spinlock)
// ==========================================
// Используется атомарный счетчик. Каждый поток "крутится" в цикле,
// пока не придет его очередь.
func methodMutex(n, taskSize int) {
	fmt.Println("\n--- Method 1: Atomic Spinlock (Active Waiting) ---")
	var current atomic.Int32
	current.Store(int32(n - 1)) // Начинаем с последнего потока

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Крутимся, пока счетчик не станет равен нашему id
			for current.Load() != int32(id) {
				runtime.Gosched() // Отдаем квант времени планировщику Go
			}

			doWork(taskSize)
			fmt.Printf("Thread %d of %d: Hello World\n", id, n)

			// Уменьшаем счетчик, передавая очередь следующему (id - 1)
			current.Add(-1)
		}(i)
	}
	wg.Wait()
}

// ==========================================
// СПОСОБ 2: Условная переменная (sync.Cond)
// ==========================================
// Потоки "засыпают" и не тратят процессорное время, пока их не разбудят.
func methodCond(n, taskSize int) {
	fmt.Println("\n--- Method 2: sync.Cond (Passive Waiting) ---")
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	current := n - 1

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			mu.Lock()
			// Ждем, пока не наступит наша очередь
			for current != id {
				cond.Wait() // Поток засыпает и освобождает мьютекс
			}

			doWork(taskSize)
			fmt.Printf("Thread %d of %d: Hello World\n", id, n)

			current--
			cond.Broadcast() // Будим все ждущие потоки, чтобы они проверили условие
			mu.Unlock()
		}(i)
	}
	wg.Wait()
}

// ==========================================
// СПОСОБ 3: Цепочка каналов (Token Passing)
// ==========================================
// Поток i ждет сигнал от потока i+1. Децентрализованная синхронизация.
func methodChanChain(n, taskSize int) {
	fmt.Println("\n--- Method 3: Channel Chain (Token Passing) ---")
	chans := make([]chan struct{}, n)
	for i := range chans {
		chans[i] = make(chan struct{}, 1)
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Все, кроме последнего, ждут сигнала от "старшего" соседа
			if id < n-1 {
				<-chans[id]
			}

			doWork(taskSize)
			fmt.Printf("Thread %d of %d: Hello World\n", id, n)

			// Передаем эстафету "младшему" соседу
			if id > 0 {
				chans[id-1] <- struct{}{}
			}
		}(i)
	}

	// Запускаем цепную реакцию с самого последнего потока
	if n > 0 {
		chans[n-1] <- struct{}{}
	}
	wg.Wait()
}

// ==========================================
// СПОСОБ 4: Массив каналов (Координатор с рукопожатием)
// ==========================================
func methodChanArray(n, taskSize int) {
	fmt.Println("\n--- Method 4: Channel Array (Coordinator with Handshake) ---")

	// Каналы для отправки сигнала "можно работать"
	startChans := make([]chan struct{}, n)
	// Каналы для ответа "я закончил печатать"
	doneChans := make([]chan struct{}, n)

	for i := range startChans {
		startChans[i] = make(chan struct{})
		doneChans[i] = make(chan struct{})
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Ждем разрешения от координатора
			<-startChans[id]

			doWork(taskSize)
			fmt.Printf("Thread %d of %d: Hello World\n", id, n)

			// Сообщаем координатору, что мы закончили
			doneChans[id] <- struct{}{}
		}(i)
	}

	// Координатор раздает разрешения в обратном порядке и ждет завершения каждого
	for i := n - 1; i >= 0; i-- {
		startChans[i] <- struct{}{} // Разрешаем работу
		<-doneChans[i]              // Ждем, пока поток напечатает и ответит
	}
	wg.Wait()
}

// ==========================================
// СПОСОБ 5: Цепочка WaitGroup
// ==========================================
func methodWGChain(n, taskSize int) {
	fmt.Println("\n--- Method 5: WaitGroup Chain ---")

	// Нам нужно n-1 вейтгрупп, чтобы поток i+1 мог разбудить поток i.
	// wgs[id] использует поток id, чтобы ждать сигнала от потока id+1.
	wgs := make([]sync.WaitGroup, n)
	for i := 0; i < n-1; i++ {
		wgs[i].Add(1) // Изначально все "закрыты" (ждут сигнала)
	}

	var masterWg sync.WaitGroup
	for i := 0; i < n; i++ {
		masterWg.Add(1)
		go func(id int) {
			defer masterWg.Done()

			// Все, кроме самого последнего (n-1), ждут сигнала от старшего соседа
			if id < n-1 {
				wgs[id].Wait()
			}

			doWork(taskSize)
			fmt.Printf("Thread %d of %d: Hello World\n", id, n)

			// Передаем эстафету младшему соседу (если он есть)
			if id > 0 {
				wgs[id-1].Done()
			}
		}(i)
	}

	// Главный поток больше не вмешивается.
	// Поток n-1 не имеет условия `id < n-1`, поэтому он не ждет,
	// сразу печатает и будит поток n-2, запуская цепную реакцию.
	masterWg.Wait()
}

func main() {
	if len(os.Args) < 3 {
		usage()
	}

	threads, err := strconv.Atoi(os.Args[1])
	if err != nil || threads <= 0 {
		fmt.Fprintf(os.Stderr, "Error: num_threads must be a positive integer\n")
		os.Exit(1)
	}

	taskSize, err := strconv.Atoi(os.Args[2])
	if err != nil || taskSize < 0 {
		fmt.Fprintf(os.Stderr, "Error: task_size must be a non-negative integer\n")
		os.Exit(1)
	}

	method := "all"
	if len(os.Args) > 3 {
		method = os.Args[3]
	}

	runtime.GOMAXPROCS(threads)
	fmt.Printf("Starting Task 3 with %d threads, task_size=%d\n", threads, taskSize)

	startTime := time.Now()

	switch method {
	case "mutex":
		methodMutex(threads, taskSize)
	case "cond":
		methodCond(threads, taskSize)
	case "chan_chain":
		methodChanChain(threads, taskSize)
	case "chan_array":
		methodChanArray(threads, taskSize)
	case "wg_chain":
		methodWGChain(threads, taskSize)
	case "all":
		methodMutex(threads, taskSize)
		methodCond(threads, taskSize)
		methodChanChain(threads, taskSize)
		methodChanArray(threads, taskSize)
		methodWGChain(threads, taskSize)
	default:
		fmt.Fprintf(os.Stderr, "Unknown method: %s\n", method)
		usage()
	}

	fmt.Printf("\nTotal execution time: %s\n", time.Since(startTime))
}
