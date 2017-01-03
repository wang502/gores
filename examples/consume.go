package main

import (
    "flag"
    "log"
    "github.com/wang502/gores/gores"
)

var (
    configPath = flag.String("c", "config.json", "path to configuration file")
)

func main(){
    flag.Parse()

    config, err := gores.InitConfig(*configPath)
    if err != nil {
        log.Fatalf("Cannot read config file")
    }

    gores.Launch(config)
}
