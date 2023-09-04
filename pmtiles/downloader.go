package pmtiles


type Task struct {
	Index int
	Rng Range
	Result chan TaskResult
}

type TaskResult struct {
	Index int
	Blob []byte
}

// returns a channel of results that exactly match the requested ranges.
func DownloadParts(getter func (Range) []byte, ranges []Range, numThreads int) chan []byte {
	intermediate := make(chan TaskResult, 8)
	orderedOutput := make(chan []byte, 8)
	tasks := make(chan Task, 100)

	lastTask := len(ranges) - 1

	worker := func (id int, tasks <-chan Task) {
	    for task := range tasks {
	        task.Result <- TaskResult{task.Index, getter(task.Rng)}
	    }
	}

	for i := 0; i < numThreads; i++ {
    go worker(i, tasks)
  }


  // push into the queue on a separate goroutine
  go func () {
	  for idx, r := range ranges {
	    tasks <- Task{Index: idx, Rng: r, Result: intermediate}
	  }
	  close(tasks)
  }()

  // a goroutine that listens on a channel
  // and buffers the results, outputting them in exact sorted order
  // once it has received all results, it closes the result channel
  go func() {
  	buffer := make(map[int]TaskResult)
  	nextIndex := 0

  	for i := range intermediate {
  		buffer[i.Index] = i

  		for {
  			if next, ok := buffer[nextIndex]; ok {
  				orderedOutput <- next.Blob
  				delete(buffer, nextIndex)
  				nextIndex++

  				if (nextIndex == lastTask) {
  					close(intermediate)
  				}
  			} else {
  				break
  			}
  		}
   	}

   	close(orderedOutput)
  }()

	return orderedOutput
}

// an number for overhead: 0.2 is 20% overhead, 1.0 is 100% overhead
// a number of maximum chunk size: n chunks * threads is the max memory usage
// store the smallest gaps in a heap; merge ranges until overhead budget is reached
func DownloadBatchedParts(getter func (Range) []byte, ranges []Range, overhead float32, maxSizeBytes int, numThreads int) chan []byte {
	orderedOutput := make(chan []byte, 8)
	return orderedOutput
}

