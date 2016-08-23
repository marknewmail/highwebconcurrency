package main

import (
	"bytes"
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
	_ "net/http/pprof"
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

func (w worker) process(client *redis.Client, j job) {
	//mu.Lock()
	// sema <- struct{}{} // acquire token
	Deduct(j.amount)
	//amount, _ := client.Get("amount").Int64()
	balance := Balance()
	var strBalance string
	if balance <= 0 {
		balance = 0
		strBalance = "0"
	} else {
		strBalance = strconv.FormatInt(j.amount, 10)
		fmt.Println(j.name+"-"+strBalance, string(j.ip), j.amount)
	}
	_, err := client.Set(j.name+"-"+strBalance, string(j.ip), 0).Result()
	if err != nil {
		log.Fatalf("write redis error: %s", err.Error())
	}
	// fmt.Printf("worker%d: completed %s!\n", w.id, j.name+"-"+strBalance)
	defer func() {
		//	mu.Unlock()
		// <-sema // release token
		j.done <- struct{}{}
	}()
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

func requestHandler(jobCh chan job, c echo.Context) error {
	// Create Job and push the work onto the jobCh.
	done := doneChanPool.Get().(chan struct{})

	job := job{[]byte(c.Request().RemoteAddress()), getUUID(), getRand(), 0, done}
	go func() {
		//fmt.Printf("added: %s %s\n", job.name, job.duration)
		jobCh <- job
	}()
	for {
		select {
		case <-job.done:
			c.Response().WriteHeader(http.StatusCreated)
			return nil
		case <-time.After(3 * time.Second):
			c.Response().WriteHeader(http.StatusRequestTimeout)
			return nil
		}
	}
	// Render success.
	defer func() {
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

	client := initClient(*maxRedisPoolSize)
	defer client.Close()
	go handleSignal(ch)
	go teller(client)
	// create job channel
	jobCh := make(chan job, *maxQueueSize)
	// create workers
	var wg sync.WaitGroup
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
	//echopprof.Wrapper(e)
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
