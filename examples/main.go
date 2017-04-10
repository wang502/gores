package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"strconv"

	"errors"

	"github.com/wang502/gores/examples/tasks"
	"github.com/wang502/gores/gores"
)

func produce(config *gores.Config) error {
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
		return errors.New("resq is nil")
	}

	err := resq.Enqueue(item)
	if err != nil {
		return fmt.Errorf("produce job failed: %s", err)
	}

	return nil
}

func consume(config *gores.Config) error {
	tasks := map[string]interface{}{
		"Item":      tasks.PrintItem,
		"Rectangle": tasks.CalculateArea,
	}
	err := gores.Launch(config, &tasks)
	if err != nil {
		return fmt.Errorf("consume item failed: %s", err)
	}

	return nil
}

func getInfo(config *gores.Config) (map[string]interface{}, error) {
	resq := gores.NewResQ(config)
	if resq == nil {
		log.Fatalf("resq is nil")
	}
	info, err := resq.Info()
	if err != nil {
		return nil, fmt.Errorf("Gores get info failed: %s", err)
	}

	return info, nil
}

func main() {
	configPath := flag.String("c", "config.json", "path to configuration file")
	option := flag.String("o", "consume", "option: weither produce or consume")
	number := flag.String("n", "0", "number of items to produce")
	flag.Parse()

	config, err := gores.InitConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	if strings.Compare("produce", *option) == 0 {
		n, _ := strconv.Atoi(*number)
		for i := 0; i < n-1; i++ {
			err = produce(config)
			if err != nil {
				log.Printf("%s", err)
			}
		}
		err = produce(config)
		if err != nil {
			log.Printf("%s", err)
		}

	} else if strings.Compare("consume", *option) == 0 {
		err = consume(config)
		if err != nil {
			log.Fatal(err)
		}

	} else if strings.Compare("info", *option) == 0 {
		info, err := getInfo(config)
		if err != nil {
			log.Fatal(err)
		}

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
