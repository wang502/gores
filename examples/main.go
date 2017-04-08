package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"strconv"

	"github.com/wang502/gores/examples/tasks"
	"github.com/wang502/gores/gores"
)

func produce(config *gores.Config) {
	item := map[string]interface{}{
		"Name":  "Rectangle",
		"Queue": "TestJob",
		"Args": map[string]interface{}{
			"Length": 10,
			"Width":  10,
		},
		"Enqueue_timestamp": time.Now().Unix(),
	}

	resq := gores.NewResQ(config)
	if resq == nil {
		log.Fatalf("resq is nil")
	}
	err := resq.Enqueue(item)
	if err != nil {
		log.Fatalf("ERROR Enqueue item to ResQ")
	}
}

func consume(config *gores.Config) {
	tasks := map[string]interface{}{
		"Item":      tasks.PrintItem,
		"Rectangle": tasks.CalculateArea,
	}
	err := gores.Launch(config, &tasks)
	if err != nil {
		log.Fatalf("ERROR launching consumer: %s\n", err)
	}
}

func getInfo(config *gores.Config) map[string]interface{} {
	resq := gores.NewResQ(config)
	if resq == nil {
		log.Fatalf("resq is nil")
	}
	info := resq.Info()
	return info
}

func main() {
	configPath := flag.String("c", "config.json", "path to configuration file")
	option := flag.String("o", "consume", "option: weither produce or consume")
	number := flag.String("n", "0", "number of items to produce")
	flag.Parse()

	config, err := gores.InitConfig(*configPath)
	if err != nil {
		log.Fatalf("Cannot read config file: %s", err)
	}

	if strings.Compare("produce", *option) == 0 {
		n, _ := strconv.Atoi(*number)
		for i := 0; i < n-1; i++ {
			produce(config)
		}
		produce(config)
	} else if strings.Compare("consume", *option) == 0 {
		consume(config)
	} else if strings.Compare("info", *option) == 0 {
		info := getInfo(config)

		fmt.Println("Gores Info: ")
		for k, v := range info {
			switch v.(type) {
			case string:
				fmt.Printf("%s : %s\n", k, v)
			case int:
				fmt.Printf("%s : %d\n", k, v)
			case int64:
				fmt.Printf("%s : %d\n", k, v)
			}
		}
	}
}
