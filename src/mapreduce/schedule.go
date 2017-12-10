package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	workerChan := make(chan string, ntasks)
	go func() {
		for {
			select {
			case newWorker := <-registerChan:
				workerChan <- newWorker
			default:
			}
		}
	}()

	taskChan := make(chan int, ntasks)
	go func() {
		for i := 0; i < ntasks; i++ {
			taskChan <- i
		}
	}()

	var wg sync.WaitGroup
	wg.Add(ntasks)
	go func() {
		for taskNumber := range taskChan {
			args := DoTaskArgs{
				JobName:       jobName,
				Phase:         phase,
				TaskNumber:    taskNumber,
				NumOtherPhase: n_other,
			}
			if phase == mapPhase {
				args.File = mapFiles[taskNumber]
			}
			go func(args DoTaskArgs) {
				worker := <-workerChan
				if call(worker, "Worker.DoTask", &args, nil) {
					workerChan <- worker
					wg.Done()
				} else {
					// Deal with worker failure
					taskChan <- args.TaskNumber
				}
			}(args)
		}
	}()

	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
