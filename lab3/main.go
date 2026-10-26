package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// vec3 используется для хранения частичных сумм по координатам.
type vec3 struct {
	x, y, z float64
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "  %s gen <task_size> [file]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <threads> <task_size> <mode> [file]\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Modes:")
	fmt.Fprintln(os.Stderr, "  loop  - parallel loop version, data decomposition")
	fmt.Fprintln(os.Stderr, "  func  - functional decomposition: sumX, sumY, sumZ")
	fmt.Fprintln(os.Stderr, "  both  - run both versions and compare results")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Examples:")
	fmt.Fprintln(os.Stderr, "  lab3 gen 2000000 points.txt")
	fmt.Fprintln(os.Stderr, "  lab3 8 2000000 loop points.txt")
	fmt.Fprintln(os.Stderr, "  lab3 8 2000000 func points.txt")
	fmt.Fprintln(os.Stderr, "  lab3 8 2000000 both points.txt")
	os.Exit(1)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fileExists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

// generatePoints создает файл с точками.
// Формат: каждая строка содержит x y z.
// Используется детерминированная генерация:
// точка i имеет координаты (i, 2*i, 3*i).
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

// readPoints читает первые need точек из файла.
// Пустые строки и строки, начинающиеся с '#', пропускаются.
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
			return nil, nil, nil, fmt.Errorf("line %d: bad x value: %v", lineNo, err)
		}

		y, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("line %d: bad y value: %v", lineNo, err)
		}

		z, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("line %d: bad z value: %v", lineNo, err)
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

// computeLoopParallel — версия 1: распараллеливание цикла.
// Это декомпозиция по данным: точки делятся между потоками непрерывными блоками.
// Каждый поток считает локальные суммы по x, y, z, затем результаты объединяются.
func computeLoopParallel(xs, ys, zs []float64, threads int) (float64, float64, float64) {
	n := len(xs)

	partials := make([]vec3, threads)
	var wg sync.WaitGroup

	chunk := (n + threads - 1) / threads
	active := 0

	for t := 0; t < threads; t++ {
		start := t * chunk
		if start >= n {
			break
		}

		end := start + chunk
		if end > n {
			end = n
		}

		active++
		wg.Add(1)

		go func(id, s, e int) {
			defer wg.Done()

			var sx, sy, sz float64

			for i := s; i < e; i++ {
				sx += xs[i]
				sy += ys[i]
				sz += zs[i]
			}

			partials[id] = vec3{x: sx, y: sy, z: sz}
		}(t, start, end)
	}

	wg.Wait()

	var sx, sy, sz float64

	for i := 0; i < active; i++ {
		sx += partials[i].x
		sy += partials[i].y
		sz += partials[i].z
	}

	return sx, sy, sz
}

// sumSlice — обычное последовательное суммирование элементов среза.
// Используется в версии с функциональной декомпозицией.
func sumSlice(vals []float64) float64 {
	var s float64
	for _, v := range vals {
		s += v
	}
	return s
}

// computeFunctional — версия 2: функциональная декомпозиция.
// Вычислительная задача делится не по данным, а по функциям:
// отдельная функция считает сумму по x, отдельная по y, отдельная по z.
// Аналог конструкции sections в OpenMP.
func computeFunctional(xs, ys, zs []float64) (float64, float64, float64) {
	var sx, sy, sz float64
	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		defer wg.Done()
		sx = sumSlice(xs)
	}()

	go func() {
		defer wg.Done()
		sy = sumSlice(ys)
	}()

	go func() {
		defer wg.Done()
		sz = sumSlice(zs)
	}()

	wg.Wait()

	return sx, sy, sz
}

// closeFloat сравнивает числа с плавающей точкой с относительной погрешностью.
func closeFloat(a, b float64) bool {
	tol := 1e-9 * math.Max(1.0, math.Max(math.Abs(a), math.Abs(b)))
	return math.Abs(a-b) <= tol
}

func centersMatch(ax, ay, az, bx, by, bz float64) bool {
	return closeFloat(ax, bx) && closeFloat(ay, by) && closeFloat(az, bz)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	// Отдельный режим генерации входного файла:
	// go run . gen <task_size> [file]
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

	// Основной режим:
	// go run . <threads> <task_size> <mode> [file]
	if len(os.Args) < 4 {
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

	mode := strings.ToLower(os.Args[3])
	switch mode {
	case "loop", "func", "both":
	default:
		fatal(fmt.Errorf("unsupported mode %q, expected loop | func | both", mode))
	}

	filename := "points.txt"
	if len(os.Args) > 4 {
		filename = os.Args[4]
	}

	runtime.GOMAXPROCS(threads)

	generatedNow := false
	if !fileExists(filename) {
		fmt.Printf("Input file %s not found. Generating %d points...\n", filename, taskSize)
		if err := generatePoints(filename, taskSize); err != nil {
			fatal(err)
		}
		generatedNow = true
	}

	readStart := time.Now()
	xs, ys, zs, err := readPoints(filename, taskSize)
	if err != nil {
		fatal(err)
	}
	readDuration := time.Since(readStart)

	n := taskSize
	nf := float64(n)

	fmt.Println("=== Lab 3: 3D geometric center ===")
	fmt.Printf("File: %s\n", filename)
	fmt.Printf("Threads: %d\n", threads)
	fmt.Printf("Task size: %d points\n", n)
	fmt.Printf("Read time: %s\n", readDuration)

	switch mode {
	case "loop":
		computeStart := time.Now()
		sx, sy, sz := computeLoopParallel(xs, ys, zs, threads)
		computeDuration := time.Since(computeStart)

		cx := sx / nf
		cy := sy / nf
		cz := sz / nf

		fmt.Println("Version: loop parallelization (data decomposition)")
		fmt.Printf("Active workers: %d\n", minInt(threads, n))
		fmt.Printf("Compute time: %s\n", computeDuration)
		fmt.Printf("Total time: %s\n", readDuration+computeDuration)
		fmt.Printf("Geometric center: x=%.6f, y=%.6f, z=%.6f\n", cx, cy, cz)

	case "func":
		computeStart := time.Now()
		sx, sy, sz := computeFunctional(xs, ys, zs)
		computeDuration := time.Since(computeStart)

		cx := sx / nf
		cy := sy / nf
		cz := sz / nf

		fmt.Println("Version: functional decomposition")
		fmt.Println("Functions: 3 (sumX, sumY, sumZ)")
		fmt.Printf("Compute time: %s\n", computeDuration)
		fmt.Printf("Total time: %s\n", readDuration+computeDuration)
		fmt.Printf("Geometric center: x=%.6f, y=%.6f, z=%.6f\n", cx, cy, cz)

	case "both":
		loopStart := time.Now()
		lsx, lsy, lsz := computeLoopParallel(xs, ys, zs, threads)
		loopDuration := time.Since(loopStart)

		funcStart := time.Now()
		fsx, fsy, fsz := computeFunctional(xs, ys, zs)
		funcDuration := time.Since(funcStart)

		lcx, lcy, lcz := lsx/nf, lsy/nf, lsz/nf
		fcx, fcy, fcz := fsx/nf, fsy/nf, fsz/nf

		match := centersMatch(lcx, lcy, lcz, fcx, fcy, fcz)

		fmt.Println("Mode: both versions")
		fmt.Println()

		fmt.Println("Version 1: loop parallelization (data decomposition)")
		fmt.Printf("Active workers: %d\n", minInt(threads, n))
		fmt.Printf("Compute time: %s\n", loopDuration)
		fmt.Printf("Total time: %s\n", readDuration+loopDuration)
		fmt.Printf("Geometric center: x=%.6f, y=%.6f, z=%.6f\n", lcx, lcy, lcz)
		fmt.Println()

		fmt.Println("Version 2: functional decomposition")
		fmt.Println("Functions: 3 (sumX, sumY, sumZ)")
		fmt.Printf("Compute time: %s\n", funcDuration)
		fmt.Printf("Total time: %s\n", readDuration+funcDuration)
		fmt.Printf("Geometric center: x=%.6f, y=%.6f, z=%.6f\n", fcx, fcy, fcz)
		fmt.Println()

		fmt.Printf("Centers match: %t\n", match)

		if !match {
			fatal(fmt.Errorf("loop and func versions produced different results"))
		}
	}

	// Если файл был только что сгенерирован программой,
	// можно показать ожидаемый геометрический центр для проверки.
	// Генератор создает точки (i, 2*i, 3*i), i = 0..n-1.
	if generatedNow {
		expectedX := float64(n-1) / 2.0
		expectedY := float64(n - 1)
		expectedZ := 3.0 * float64(n-1) / 2.0

		fmt.Println()
		fmt.Println("Expected center for generated file:")
		fmt.Printf("x=%.6f, y=%.6f, z=%.6f\n", expectedX, expectedY, expectedZ)
	}
}
