package main

import (
	"bytes"
	"context"
	"crypto/rand"
	_ "expvar"
	"flag"
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	//"github.com/go-redis/redis"
	"net/http"
	"runtime"
	"time"

	"gopkg.in/redis.v4"

	"github.com/labstack/echo"
	"github.com/labstack/echo/engine"
	"github.com/labstack/echo/engine/fasthttp"
)

type job struct {
	ip       []byte
	name     string
	amount   int64
	duration time.Duration
	done     chan struct{}
}

var mu sync.Mutex
var (
	sema = make(chan struct{}, 1) // a binary semaphore guarding balance
)

// type Once struct {
// 	done int32
// }

// func (o *Once) Do(f func()) {
// 	if atomic.LoadInt32(&o.done) == 1 {
// 		return
// 	}
// 	// Slow-path.
// 	if atomic.CompareAndSwapInt32(&o.done, 0, 1) {
// 		f()
// 	}
// }

// func Deposit(amount int) {
// 	sema <- struct{}{} // acquire token
// 	balance = balance + amount
// 	<-sema // release token
// }

// func Balance() int {
// 	sema <- struct{}{} // acquire token
// 	b := balance
// 	<-sema // release token
// 	return b
// }

type worker struct {
	id int
}

var allocates = make(chan int64, 20) // send amount to allocate
var balances = make(chan int64, 20)  // receive balance

func Deduct(amount int64) { allocates <- amount }
func Balance() int64      { return <-balances }

func teller(client *redis.Client) {
	var balance int64 // balance is confined to teller goroutine
	for {
		select {
		case amount := <-allocates:
			balance, _ := client.Get("amount").Int64()
			if balance > 0 {
				balance -= amount
				if balance < 0 {
					balance += amount
				}
				strBalance := strconv.FormatInt(balance, 10)
				client.Set("amount", strBalance, 0)
				balances <- balance
			}
		case balances <- balance:
		}
	}
}

var startTime int64
var runned bool

func (w worker) process(client *redis.Client, taskjob job) error {
	// mu.Lock()
	// sema <- struct{}{} // acquire token
	///Deduct(j.amount)
	//amount, _ := client.Get("amount").Int64()
	///balance := Balance()
	var decrby func(string, int64, int) error
	retrytimes := 3
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(time.Second*1))

	decrby = func(key string, value int64, retrytimes int) error {
		if value <= 0 {
			return nil
		}
		err := client.Watch(func(tx *redis.Tx) error {
			var once sync.Once
			onceBody := func() {
				fmt.Printf("%.2f抢光!\n", float32(time.Now().UnixNano()-startTime)/1e9)
			}
			n, err := tx.Get(key).Int64()
			if n == 0 {
				if !runned {
					once.Do(onceBody)
					runned = true
				}
				//os.Exit(0)
				return nil
			}
			if err != nil && err != redis.Nil {
				return err
			}
			if n-value >= 0 {
				_, err = tx.MultiExec(func() error {
					tx.DecrBy(key, value)
					fmt.Println(strconv.FormatInt(n-value, 10))
					// tx.Set(key, strconv.FormatInt(n-value, 10), 0)
					return nil
				})
			}
			strValue := strconv.FormatInt(taskjob.amount, 10)
			_, err = client.Set(taskjob.name+"-"+strValue, string(taskjob.ip), 0).Result()
			// if err == nil && n-value >= 0 {
			// 	fmt.Printf("任务:%s 额度:%d 金额:%d 剩余:%d\n", taskjob.name, n, value, n-value)
			// }
			return err

		}, key)
		if err == redis.TxFailedErr && retrytimes > 0 {
			retrytimes--
			time.Sleep(time.Microsecond)
			return decrby(key, value, retrytimes)
			// return nil
		}

		return err
	}

	err := decrby("amount", taskjob.amount, retrytimes)
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
	}

	// var strBalance string
	// if balance <= 0 {
	// 	balance = 0
	// 	strBalance = "0"
	// } else {
	// 	strBalance = strconv.FormatInt(j.amount, 10)
	// 	fmt.Println(j.name+"-"+strBalance, string(j.ip), j.amount)
	// }
	// _, err := client.Set(j.name+"-"+strBalance, string(j.ip), 0).Result()
	// if err != nil {
	// 	log.Fatalf("write redis error: %s", err.Error())
	// }
	// fmt.Printf("worker%d: completed %s!\n", w.id, j.name+"-"+strBalance)
	defer func() {
		// mu.Unlock()
		cancelFunc()
		taskjob.done <- struct{}{}
		// <-sema // release token
	}()
	return err
}

var textBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 16)) // 16bytes
	},
}
var doneChanPool = sync.Pool{
	New: func() interface{} {
		return make(chan struct{})
	},
}

func getRand() int64 {
	mrand.Seed(time.Now().UnixNano())
	return int64(mrand.Intn(10000))
}

func getUUID() (uuid string) {
	buf := textBufferPool.Get().(*bytes.Buffer).Bytes()
	_, err := rand.Read(buf)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	uuid = fmt.Sprintf("%X-%X-%X-%X-%X", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:])
	//uuid = string(buf[0:4]) + "-" + string(buf[4:6]) + "-" + string(buf[6:8]) + "-" + string(buf[8:10]) + "-" + string(buf[10:])
	return
}

func requestHandler(jobCh chan<- job, c echo.Context) error {
	// Create Job and push the work onto the jobCh.
	done := doneChanPool.Get().(chan struct{})
	taskjob := job{[]byte(c.Request().RemoteAddress()), getUUID(), getRand(), 0, done}
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(time.Second*1))
	go func(taskjob job, ctx context.Context) {
		//fmt.Printf("added: %s %s\n", job.name, job.duration)
		jobCh <- taskjob
	}(taskjob, ctx)

	for {
		select {
		case <-taskjob.done:
			c.Response().WriteHeader(http.StatusCreated)
			return nil
		case <-time.After(1 * time.Second):
			<-ctx.Done()
			c.Response().WriteHeader(http.StatusRequestTimeout)
			return nil
		}
	}
	// }(taskjob)

	// Render success.
	defer func() {
		cancelFunc()
		if x := recover(); x != nil {
			log.Printf("[%v] caught panic :%v", c.Request().RemoteAddress(), x)
		}
	}()
	return nil
}

func initClient(poolSize int) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:         "0.0.0.0:6379",
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     poolSize,
		Password:     "",
		DB:           0,
	})
	// if err := client.FlushDb().Err(); err != nil {
	// 	panic(err)
	// }
	return client
}
func main() {
	var (
		maxQueueSize     = flag.Int("max_queue_size", 100, "The size of job queue")
		maxRedisPoolSize = flag.Int("max_redis_pool_size", 20, "The pool size for redis")
		maxWorkers       = flag.Int("max_workers", 5, "The number of workers to start")
		port             = flag.String("port", "0.0.0.0:8080", "The server port")
		ch               = make(chan bool)
	)
	flag.Parse()
	runned = false

	client := initClient(*maxRedisPoolSize)
	defer client.Close()
	go handleSignal(ch)
	// go teller(client)
	// fmt.Println(time.Now())
	startTime = time.Now().UnixNano()
	// create job channel
	jobCh := make(chan job, *maxQueueSize)
	// create workers
	var wg sync.WaitGroup
	*maxWorkers = runtime.NumCPU()
	fmt.Println(*maxWorkers)
	for i := 0; i < *maxWorkers; i++ {
		w := worker{i}
		wg.Add(1)
		go func(client *redis.Client, w worker, jobCh <-chan job, wg *sync.WaitGroup) {
			for j := range jobCh {
				w.process(client, j)
			}
			defer wg.Done()
		}(client, w, jobCh, &wg)
	}

	go func() {
		wg.Wait()
		close(jobCh)
	}()

	e := echo.New()
	cfg := engine.Config{
		Address:      *port,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	e.POST("/work", func(c echo.Context) error {
		// v, _ := json.Marshal(data)
		// fmt.Println(data, "\n", string(v))
		return requestHandler(jobCh, c)
		//return c.JSON(http.StatusOK, data)
	})
	// automatically add routers for net/http/pprof
	// e.g. /debug/pprof, /debug/pprof/heap, etc.
	// echopprof.Wrapper(e)
	go func() {
		e.Run(fasthttp.WithConfig(cfg))
	}()
	<-ch
}

func handleSignal(srcCh chan bool) {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGTERM)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGHUP:
		//log.Println("[ OS][ TERM] - server sighup ...")
		case syscall.SIGTERM: // 退出时
			log.Println("[ OS][ TERM] - server has exit !")
			close(srcCh)
		}
	}
}
