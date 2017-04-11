package tasks

import (
	"errors"
	"fmt"
)

// PrintItem is a task for item with name 'Item'
func PrintItem(args map[string]interface{}) error {
	for k, v := range args {
		fmt.Printf("key: %s, value: %s\n", k, v)
	}

	return nil
}

// CalculateArea is a task for item with 'Name' = 'Rectangle'
func CalculateArea(args map[string]interface{}) error {
	length, ok1 := args["Length"]
	width, ok2 := args["Width"]
	if !ok1 || !ok2 {
		return errors.New("Map has no required attributes")
	}

	fmt.Printf("The rectangle area is %d\n", int(length.(float64))*int(width.(float64)))
	return nil
}
