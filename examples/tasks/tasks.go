package tasks

import (
    "errors"
    "fmt"
)

// Task for dequeued item with 'Name' = 'Item'
func PrintItem(args map[string]interface{}) error {
    var err error
    for k, v := range args {
        fmt.Printf("key: %s, value: %s\n", k, v)
    }
    return err
}

// task for item with 'Name' = 'Rectangle'
func CalculateArea(args map[string]interface{}) error {
    var err error

    length := args["Length"]
    width := args["Width"]
    if length == nil || width == nil {
        err = errors.New("Map has no required attributes")
        return err
    }
    fmt.Printf("The rectangle area is %d\n", int(length.(float64)) * int(width.(float64)))
    return err
}
