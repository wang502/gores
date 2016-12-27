package gores

type Job struct {
    queue string
    payload map[string]interface{}
    worker string
}
