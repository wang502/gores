package tasks

import (
    "errors"
    "fmt"
)

// Task for dequeued item with 'Name' = 'Item'
func PrintItem(item map[string]interface{}) error {
    var err error
    for k, v := range item {
        fmt.Printf("key: %s, value: %s\n", k, v)
    }
    return err
}

// task for item with 'Name' = 'Rectangle'
func CalculateArea(item map[string]interface{}) error {
    var err error

    length := item["Length"]
    width := item["Width"]
    if length == nil || width == nil {
        err = errors.New("Map has no required attributes")
        return err
    }
    fmt.Printf("The area is %d\n", int(length.(float64)) * int(width.(float64)))
    return err
}
