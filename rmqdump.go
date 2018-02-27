package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/michaelklishin/rabbit-hole"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"unsafe"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s\n", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

type Config struct {
	Host                string `toml:"host"`
	User                string `toml:"user"`
	Password            string `toml:"password"`
	Vhost               string `toml:"vhost"`
	Output              string `toml:"output"`
	RotationBytes       int64  `toml:"rotation_bytes"`
	ExpireMilliSec      int32  `toml:"expire_millisec"`
	MaxQueueLength      int32  `toml:"max_queue_length"`
	MaxQueueLengthBytes int64  `toml:"max_queue_length_bytes"`
	TraceQueue          string `toml:"trace_queue"`
}

func loadConfig(path string) *Config {
	cfg := Config{}
	cfg.TraceQueue = "rmqdump"
	cfg.ExpireMilliSec = 120000
	cfg.MaxQueueLength = 10000
	cfg.MaxQueueLengthBytes = 20000000
	_, err := toml.DecodeFile(path, &cfg)
	failOnError(err, "Failed to load config")
	return &cfg
}

func (cfg *Config) getURI() string {
	uri := amqp.URI{
		Scheme:   "amqp",
		Host:     cfg.Host,
		Port:     5672,
		Username: cfg.User,
		Password: cfg.Password,
		Vhost:    cfg.Vhost}
	return uri.String()
}

func (cfg *Config) getNameForDeadLetter() string {
	return cfg.TraceQueue + "-dl"
}

func enableTracing(cfg *Config) error {
	uri := fmt.Sprintf("http://%s:15672", cfg.Host)
	rmqc, _ := rabbithole.NewClient(uri, cfg.User, cfg.Password)
	res, err := rmqc.GetVhost(cfg.Vhost)
	if err != nil {
		return err
	}
	if res.Tracing {
		return nil
	}
	_, err = rmqc.PutVhost(cfg.Vhost, rabbithole.VhostSettings{Tracing: true})
	return err
}

func declareQueues(channel *amqp.Channel, cfg *Config) error {
	dlx := cfg.getNameForDeadLetter()
	dlq := cfg.getNameForDeadLetter()
	err := channel.ExchangeDeclare(dlx, "fanout", false, true, false, false, nil)
	if err != nil {
		return err
	}
	dlqArgs := amqp.Table{
		"x-expires":    cfg.ExpireMilliSec,
		"x-max-length": int32(1),
	}
	_, err = channel.QueueDeclare(dlq, false, false, false, false, dlqArgs)
	if err != nil {
		return err
	}
	err = channel.QueueBind(dlq, "", dlx, false, nil)
	if err != nil {
		return err
	}
	qArgs := amqp.Table{
		"x-expires":              cfg.ExpireMilliSec,
		"x-max-length":           cfg.MaxQueueLength,
		"x-max-length-bytes":     cfg.MaxQueueLengthBytes,
		"x-dead-letter-exchange": dlx,
	}
	_, err = channel.QueueDeclare(cfg.TraceQueue, false, false, false, false, qArgs)
	if err != nil {
		return err
	}
	err = channel.QueueBind(cfg.TraceQueue, "publish.#", "amq.rabbitmq.trace", false, nil)
	if err != nil {
		return err
	}
	return nil
}

type Record struct {
	Number     int
	Time       time.Time
	Delivery   *amqp.Delivery
	DeadLetter bool
}

type StopCommand struct{}
type ReopenCommand struct{}

func isBase64Needed(d *amqp.Delivery) bool {
	pi, ok := d.Headers["properties"]
	if !ok {
		return true
	}
	if pt, ok := pi.(amqp.Table); ok {
		val := pt["content_encoding"]
		if x, ok := val.(string); ok && x == "utf-8" {
			return false
		}
	}
	return true
}

func reopen(fileName string, currentFile *os.File) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return currentFile, err
	} else {
		if currentFile != nil {
			currentFile.Close()
		}
		return file, nil
	}
}

func getFileSize(file *os.File) int64 {
	if stat, err := file.Stat(); err == nil {
		return stat.Size()
	} else {
		return -1
	}
}

func append(baseFileName string, rotationBytes int64, recordC <-chan Record, reopenC <-chan ReopenCommand) {
	file, err := reopen(baseFileName, nil)
	failOnError(err, "could not open")
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	enc := json.NewEncoder(file)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
FOR:
	for {
		select {
		case record, ok := <-recordC:
			if !ok {
				break FOR
			}
			record2 := map[string]interface{}{
				"trace_number":      record.Number,
				"trace_unixtime":    float64(record.Time.UnixNano()) / 1000000000,
				"trace_time":        record.Time.Format("2006-01-02T15:04:05.000000000Z07:00"),
				"trace_routing_key": record.Delivery.RoutingKey,
				"headers":           record.Delivery.Headers,
				"dead_letter":       record.DeadLetter,
			}
			isBase64 := isBase64Needed(record.Delivery)
			record2["base64"] = isBase64
			if isBase64 {
				// passing []byte to the encoder results in base64
				record2["body"] = record.Delivery.Body
			} else {
				record2["body"] = *(*string)(unsafe.Pointer(&record.Delivery.Body))
			}
			enc.Encode(record2)
		case <-ticker.C:
			if rotationBytes > 0 && getFileSize(file) > rotationBytes {
				archiveFileName := baseFileName + "-" + time.Now().UTC().Format("20060102-150405")
				err := os.Rename(baseFileName, archiveFileName)
				if err != nil {
					log.Printf("Error in Rename: %s\n", err)
				} else {
					file, err = reopen(baseFileName, file)
					failOnError(err, "could not open")
					enc = json.NewEncoder(file)
					go gzipFile(archiveFileName, archiveFileName+".gz")
				}
			}
		case <-reopenC:
			file, err = reopen(baseFileName, file)
			failOnError(err, "could not open")
			enc = json.NewEncoder(file)
		}
	}
}

func gzipFile(src string, dst string) {
	log.Printf("Compress %s\n", src)
	srcF, err := os.Open(src)
	if err != nil {
		log.Printf("Error in Open: %s\n", err)
		return
	}
	defer srcF.Close()
	dstF, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		log.Printf("Error in Create: %s\n", err)
		return
	}
	defer dstF.Close()
	writer := gzip.NewWriter(dstF)
	defer writer.Close()
	buf := bufio.NewWriter(writer)
	defer buf.Flush()
	bytes := make([]byte, 65536)
	for {
		n, err := srcF.Read(bytes)
		if n == 0 {
			break
		}
		if err != nil {
			log.Printf("Error in Read: %s\n", err)
		}
		buf.Write(bytes[:n])
	}
	os.Remove(src)
	log.Printf("Compress %s: done\n", src)
}

func startConsume(cfg *Config, stopC <-chan StopCommand) <-chan Record {
	recordC := make(chan Record, 10000)
	go func() {
		defer close(recordC)
		conn, err := amqp.Dial(cfg.getURI())
		if err != nil {
			log.Printf("Failed to connect to MQ: %s\n", err)
			return
		}
		defer conn.Close()
		log.Println("connected")
		channel, err := conn.Channel()
		if err != nil {
			log.Printf("Failed to open a channel: %s\n", err)
			return
		}
		declareQueues(channel, cfg)
		messageC, err := channel.Consume(cfg.TraceQueue, "", true, false, false, false, nil)
		if err != nil {
			log.Printf("Failed to consume trace queue: %s\n", err)
			return
		}
		deadMessageC, err := channel.Consume(cfg.getNameForDeadLetter(), "", true, false, false, false, nil)
		if err != nil {
			log.Printf("Failed to consume dead letter queue: %s\n", err)
			return
		}
		count := 0
		for {
			select {
			case data, ok := <-messageC:
				if !ok {
					return
				}
				recordC <- Record{
					Number:     count,
					Time:       time.Now().UTC(),
					Delivery:   &data,
					DeadLetter: false,
				}
				count++
			case data, ok := <-deadMessageC:
				if !ok {
					return
				}
				recordC <- Record{
					Number:     count,
					Time:       time.Now().UTC(),
					Delivery:   &data,
					DeadLetter: true,
				}
				count++
			case <-stopC:
				log.Print("stop\n")
				return
			}
		}
	}()
	return recordC
}

func consumeForever(cfg *Config, stopC <-chan StopCommand) <-chan Record {
	recordC := make(chan Record)
	go func() {
		defer close(recordC)
		consume := func() error {
			err := enableTracing(cfg)
			if err != nil {
				return err
			}
			stopSubC := make(chan StopCommand)
			defer close(stopSubC)
			recordSubC := startConsume(cfg, stopSubC)
			for {
				select {
				case record, ok := <-recordSubC:
					if !ok {
						return errors.New("consumer stopped unexpectedly")
					}
					recordC <- record
				case <-stopC:
					stopSubC <- StopCommand{}
					return nil
				}
			}
		}
		for {
			err := consume()
			if err == nil {
				return
			}
			log.Printf("Error in consume: %s\n", err)
			time.Sleep(time.Second * 1)
		}
	}()
	return recordC
}

func signalsToCommands() (<-chan StopCommand, <-chan ReopenCommand) {
	stopC := make(chan StopCommand)
	reopenC := make(chan ReopenCommand)
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for signal := range signals {
			log.Printf("caught signal: %s\n", signal)
			if signal == syscall.SIGHUP {
				reopenC <- ReopenCommand{}
			} else {
				stopC <- StopCommand{}
			}
		}
	}()
	return stopC, reopenC
}

func main() {
	configPath := flag.String("config", "config.toml", "path to config.toml")
	flag.Parse()
	cfg := loadConfig(*configPath)
	stopC, reopenC := signalsToCommands()
	recordC := consumeForever(cfg, stopC)
	append(cfg.Output, cfg.RotationBytes, recordC, reopenC)
}
