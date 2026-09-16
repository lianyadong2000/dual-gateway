package main

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ==================== 第一部分：基础协程 ====================

// 1. 最简单的协程
func basicGoroutine() {
	fmt.Println("\n=== 1. 基础协程 ===")

	// 使用go关键字启动协程
	go func() {
		fmt.Println("Hello from goroutine!")
	}()

	// 等待协程执行（不推荐实际使用，仅用于演示）
	time.Sleep(100 * time.Millisecond)
	fmt.Println("Hello from main!")
}

// 2. 多个协程并发
func multipleGoroutines() {
	fmt.Println("\n=== 2. 多个协程并发 ===")

	var wg sync.WaitGroup

	// 启动10个协程
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			fmt.Printf("Goroutine %d finished\n", id)
		}(i)
	}

	wg.Wait()
	fmt.Println("All goroutines finished!")
}

// ==================== 第二部分：协程调度 ====================

// 3. 查看GOMAXPROCS
func showGOMAXPROCS() {
	fmt.Println("\n=== 3. GOMAXPROCS ===")

	// 获取当前CPU核心数
	numCPU := runtime.NumCPU()
	fmt.Printf("Number of CPU cores: %d\n", numCPU)

	// 设置GOMAXPROCS
	runtime.GOMAXPROCS(4)
	fmt.Printf("GOMAXPROCS set to: %d\n", runtime.GOMAXPROCS(0))

	// 查看当前goroutine数量
	fmt.Printf("Number of goroutines: %d\n", runtime.NumGoroutine())
}

// 4. 协程调度演示 - 让出CPU
func goroutineScheduling() {
	fmt.Println("\n=== 4. 协程调度 ===")

	// 使用runtime.Gosched()让出CPU
	done := make(chan bool)

	go func() {
		for i := 0; i < 5; i++ {
			fmt.Printf("Goroutine 1: iteration %d\n", i)
			if i == 2 {
				fmt.Println("Goroutine 1: calling Gosched()")
				runtime.Gosched() // 让出CPU，让其他协程运行
			}
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 5; i++ {
			fmt.Printf("Goroutine 2: iteration %d\n", i)
		}
		done <- true
	}()

	<-done
	<-done
}

// ==================== 第三部分：协程同步 ====================

// 5. 使用WaitGroup
func waitGroupExample() {
	fmt.Println("\n=== 5. WaitGroup同步 ===")

	var wg sync.WaitGroup

	// 启动多个工作协程
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go worker(i, &wg)
	}

	wg.Wait()
	fmt.Println("All workers done!")
}

func worker(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Printf("Worker %d starting\n", id)
	time.Sleep(time.Duration(id) * time.Second)
	fmt.Printf("Worker %d done\n", id)
}

// 6. 使用Mutex保护共享数据
func mutexExample() {
	fmt.Println("\n=== 6. Mutex互斥锁 ===")

	var (
		counter int
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	// 启动100个协程并发增加counter
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			counter++
			mu.Unlock()
		}()
	}

	wg.Wait()
	fmt.Printf("Counter value: %d (expected: 100)\n", counter)
}

// 7. 使用RWMutex
func rwMutexExample() {
	fmt.Println("\n=== 7. RWMutex读写锁 ===")

	var (
		data map[string]string
		mu   sync.RWMutex
		wg   sync.WaitGroup
	)

	data = make(map[string]string)

	// 写操作
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			key := fmt.Sprintf("key%d", id)
			data[key] = fmt.Sprintf("value%d", id)
			fmt.Printf("Write: %s -> %s\n", key, data[key])
			time.Sleep(100 * time.Millisecond)
		}(i)
	}

	// 读操作
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mu.RLock()
			defer mu.RUnlock()
			key := fmt.Sprintf("key%d", id%5)
			if val, ok := data[key]; ok {
				fmt.Printf("Read: %s = %s\n", key, val)
			}
			time.Sleep(50 * time.Millisecond)
		}(i)
	}

	wg.Wait()
}

// ==================== 第四部分：协程通信 ====================

// 8. 使用Channel通信
func channelExample() {
	fmt.Println("\n=== 8. Channel通信 ===")

	// 创建有缓冲channel
	ch := make(chan int, 5)

	// 生产者
	go func() {
		for i := 0; i < 5; i++ {
			ch <- i
			fmt.Printf("Produced: %d\n", i)
			time.Sleep(100 * time.Millisecond)
		}
		close(ch)
	}()

	// 消费者
	for v := range ch {
		fmt.Printf("Consumed: %d\n", v)
		time.Sleep(150 * time.Millisecond)
	}
}

// 9. Select多路复用
func selectExample() {
	fmt.Println("\n=== 9. Select多路复用 ===")

	ch1 := make(chan string)
	ch2 := make(chan string)

	// 发送数据到两个channel
	go func() {
		time.Sleep(100 * time.Millisecond)
		ch1 <- "from channel 1"
	}()

	go func() {
		time.Sleep(200 * time.Millisecond)
		ch2 <- "from channel 2"
	}()

	// 使用select等待多个channel
	for i := 0; i < 2; i++ {
		select {
		case msg1 := <-ch1:
			fmt.Printf("Received: %s\n", msg1)
		case msg2 := <-ch2:
			fmt.Printf("Received: %s\n", msg2)
		case <-time.After(300 * time.Millisecond):
			fmt.Println("Timeout!")
		}
	}
}

// ==================== 第五部分：高级模式 ====================

// 10. 工作池模式
func workerPoolExample() {
	fmt.Println("\n=== 10. 工作池模式 ===")

	const (
		numWorkers = 3
		numJobs    = 10
	)

	jobs := make(chan int, numJobs)
	results := make(chan int, numJobs)

	// 启动工作协程池
	var wg sync.WaitGroup
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go workerPool(w, jobs, results, &wg)
	}

	// 发送任务
	for j := 1; j <= numJobs; j++ {
		jobs <- j
	}
	close(jobs)

	// 等待所有工作者完成
	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果
	for result := range results {
		fmt.Printf("Result: %d\n", result)
	}
}

func workerPool(id int, jobs <-chan int, results chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		fmt.Printf("Worker %d processing job %d\n", id, job)
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		results <- job * 2
	}
}

// 11. 扇出/扇入模式
func fanOutInExample() {
	fmt.Println("\n=== 11. 扇出/扇入模式 ===")

	// 数据源
	input := make(chan int)

	// 启动数据生成器
	go func() {
		for i := 0; i < 10; i++ {
			input <- i
		}
		close(input)
	}()

	// 扇出：启动多个处理器
	outputs := make([]<-chan int, 3)
	for i := 0; i < 3; i++ {
		outputs[i] = processor(input, i+1)
	}

	// 扇入：合并结果
	merged := merge(outputs...)

	// 收集结果
	for result := range merged {
		fmt.Printf("Final result: %d\n", result)
	}
}

func processor(in <-chan int, id int) <-chan int {
	out := make(chan int)

	go func() {
		for n := range in {
			fmt.Printf("Processor %d processing %d\n", id, n)
			time.Sleep(50 * time.Millisecond)
			out <- n * id
		}
		close(out)
	}()

	return out
}

func merge(channels ...<-chan int) <-chan int {
	var wg sync.WaitGroup
	out := make(chan int)

	// 为每个channel启动一个协程
	for _, ch := range channels {
		wg.Add(1)
		go func(c <-chan int) {
			defer wg.Done()
			for n := range c {
				out <- n
			}
		}(ch)
	}

	// 等待所有协程完成后关闭out
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// 12. Context取消协程
func contextExample() {
	fmt.Println("\n=== 12. Context取消协程 ===")

	ctx, cancel := context.WithCancel(context.Background())

	// 启动长时间运行的协程
	go longRunningTask(ctx)

	// 运行2秒后取消
	time.Sleep(2 * time.Second)
	cancel()

	// 等待协程退出
	time.Sleep(500 * time.Millisecond)
	fmt.Println("Main: context cancelled")
}

func longRunningTask(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Println("Task: working...")
		case <-ctx.Done():
			fmt.Println("Task: cancelled!")
			return
		}
	}
}

// 13. 原子操作
func atomicExample() {
	fmt.Println("\n=== 13. 原子操作 ===")

	var counter int64
	var wg sync.WaitGroup

	// 使用原子操作增加counter
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddInt64(&counter, 1)
		}()
	}

	wg.Wait()
	fmt.Printf("Counter: %d (using atomic)\n", counter)
}

// 14. Once单次执行
func onceExample() {
	fmt.Println("\n=== 14. Once单次执行 ===")

	var once sync.Once
	var wg sync.WaitGroup

	// 多个协程尝试执行相同的初始化
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			once.Do(func() {
				fmt.Printf("Initialization from goroutine %d\n", id)
			})
		}(i)
	}

	wg.Wait()
}

// 15. 协程泄漏检测
func goroutineLeakDetection() {
	fmt.Println("\n=== 15. 协程泄漏检测 ===")

	// 创建一个永不关闭的channel
	ch := make(chan int)

	// 启动协程，这个协程会永久阻塞（泄漏）
	go func() {
		fmt.Println("Leaky goroutine started")
		<-ch // 永远等待
		fmt.Println("This will never print")
	}()

	// 查看当前协程数量
	fmt.Printf("Goroutines before: %d\n", runtime.NumGoroutine())

	// 等待一会儿
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("Goroutines after: %d (includes leaky goroutine)\n", runtime.NumGoroutine())

	// 注意：实际开发中应该使用工具检测协程泄漏
	fmt.Println("Tip: Use 'go tool pprof' to detect goroutine leaks")
}

// ==================== 主函数 ====================

func main() {
	fmt.Println("========== Go协程深入学习 ==========")
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("NumCPU: %d\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))

	// 设置随机种子
	rand.Seed(time.Now().UnixNano())

	// 运行所有示例
	basicGoroutine()
	multipleGoroutines()
	showGOMAXPROCS()
	goroutineScheduling()
	waitGroupExample()
	mutexExample()
	rwMutexExample()
	channelExample()
	selectExample()
	workerPoolExample()
	fanOutInExample()
	contextExample()
	atomicExample()
	onceExample()
	goroutineLeakDetection()

	fmt.Println("\n========== 学习完成 ==========")
	fmt.Println("建议进一步学习:")
	fmt.Println("1. 使用'go run -race'检测数据竞争")
	fmt.Println("2. 使用'go tool trace'分析协程调度")
	fmt.Println("3. 阅读官方文档: https://golang.org/doc/effective_go#concurrency")
}
