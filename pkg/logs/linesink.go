package logs

// LineSink receives parsed log lines from pod followers. A single goroutine
// in StreamWithSink calls WriteLine and Ping.
type LineSink interface {
	WriteLine(line LogLine) error
	Ping() error
}
