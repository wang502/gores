package gores

import (
    "fmt"
    "reflect"
)

// Item for testing
type TestItem struct{
    Name string `json:"Name"`
    Queue string `json:"Queue"`
    Args map[string]interface{} `json:"Args"`
    Enqueue_timestamp int64 `json:"Enqueue_timestamp"`
}

func (t *TestItem) string() string{
    return t.Name
}

func (item TestItem) Perform(args map[string]interface{}) error {
    for k, v := range args {
        fmt.Printf("key: %s value: %s \n", k, v)
    }
    return nil
}

/* ---------------------------------------------------------------------- */
var typeRegistry = make(map[string]reflect.Type)

func InitRegistry(){
    // build your own registry by mapping a struct name with the struct type
    // ex. typeRegistry["Item"] = reflect.TypeOf(Item{})
    typeRegistry["TestItem"] = reflect.TypeOf(TestItem{})
}

func StrToInstance(name string) interface{}{
    t := typeRegistry[name]
    if t == nil {
        return nil
    }
    return reflect.New(t).Elem().Interface()
}

func InstancePerform(instance interface{}, args map[string]interface{}) error{
    var err error
    switch instance.(type) {
        case TestItem:
            err = instance.(TestItem).Perform(args)
    }
    return err
}

func InstanceAfterPerform(metadata map[string]interface{}) error {
    /* an ``InstanceAfterPerform`` method is called after
        ``Perform`` is finished.  The metadata map contains the
        same data, plus a timestamp of when the job was performed, a
        ``failed`` boolean value, and if it did fail, a ``retried``
        boolean value.  This method is called after retry, and is
        called regardless of whether an exception is ultimately thrown
        by the Perform method.
    */
    return nil
}
