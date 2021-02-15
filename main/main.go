package main

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	mainThreadIdleTime   = 40 //Main thread will be up until this much time - in seconds
	numberOfTasks        = 99 //Number of tasks to be added to queue
	statusFailed         = "FAILED"
	statusCompleted      = "COMPLETED"
	statusPending        = "PENDING"
	statusTimeout        = "TIMEOUT"
	retryThreshold       = 5  //Number of retries for failure cases
	taskTimeOutInSeconds = 20 //IN SECONDS .Each task will be in the queue to a maximum of this configration in settings. After that the task will be marked as TIMEOUT and removed .

	//FailureTaskIds is to configure tasks which are needed to be marked as FAILED.
	failureTaskIds = "TASK_18_ID,TASK_21_ID,TASK_22_ID,TASK_54_ID"
	//skipTaskIdsForTimeout is to configure tasks which are needed to be marked as TIMEOUT.
	skipTaskIdsForTimeout = "TASK_16_ID,TASK_25_ID,TASK_23_ID,TASK_88_ID"
)

type Task struct {
	Id          string
	status      string
	CreatedTime time.Time
	Data        string //Not using this field as of now as I dont have any logic to process the DATA
	Retries     int
}
type Queue struct {
	tasks []Task
	mu    sync.Mutex
}

type QueueOperation interface {
	Push(task Task) *Queue
	Pop() *Queue
}

func main() {
	var tasks []Task
	//Adding tasks to queue  , incremental number will be the task ID
	for i := 0; i < numberOfTasks; i++ {
		tasks = append(tasks, Task{Id: fmt.Sprintf("TASK_%d_ID", i), status: statusPending, CreatedTime: time.Now(), Data: "", Retries: 0})
	}

	queue := Queue{tasks: tasks}

	go queue.Executor(&queue)
	go queue.Cleaner(&queue)

	//Just to keep the main thread up until the goroutines are complete. We can use scan line instead
	time.Sleep(mainThreadIdleTime * time.Second)
}

func (q *Queue) Executor(queue *Queue) {
	log.Println("Initial count for executor: ", len(queue.tasks))
	//To keep the goroutine look into the queue until the queue is empty
	ch := make(chan Task)
	for ; len(queue.tasks) > 0; {
		q.mu.Lock()
		if len(queue.tasks) > 0 {
			go processTask(ch)
			ch <- queue.tasks[0]
			queue.tasks[0] = <-ch
		} else {
			q.mu.Unlock()
			break
		}
		q.mu.Unlock()
	}
	log.Println("Executor Exited")
}

func processTask(ch chan Task) {
	task := <-ch
	//Marking the task as TIMEOUT if it is idle for the threshold timeout
	if task.CreatedTime.Add(taskTimeOutInSeconds * time.Second).Before(time.Now()) {
		task.status = statusTimeout
	}
	//constructing some test scenarios.
	if strings.Contains(failureTaskIds, task.Id) {
		if task.Retries < retryThreshold {
			task.status = statusFailed
			task.Retries = task.Retries + 1
			log.Println("Task failed :", task.Id, " With retry count :", task.Retries)
		}
	} else if !strings.Contains(skipTaskIdsForTimeout, task.Id) {
		task.status = statusCompleted
	}
	ch <- task
}

func (q *Queue) Cleaner(queue *Queue) {
	log.Println("Initial count for Cleaner: ", len(queue.tasks))
	//To keep the goroutine look into the queue until the queue is empty
	for len(queue.tasks) > 0 {
		q.mu.Lock()
		task := queue.tasks[0]
		switch task.status {
		case statusPending:
			{ //this is a minor tweak to test scenario as PENDING item will be re-added to the end of queue if it is in the timeout test id's
				//statusPending CASE here is STRICTLY for creating test scenario
				if strings.Contains(skipTaskIdsForTimeout, task.Id) {
					dequeue(q)
					enqueue(q, task)
				}
			}
		case statusFailed:
			{
				if task.Retries < retryThreshold {
					log.Println("Re-adding the FAILED task as the retry count is less than threshold:", task)
					dequeue(q)
					enqueue(q, task)
				} else {
					log.Println("Removing the FAILED task after retries ID :", task.Id, "  TASK :", task)
					dequeue(q)
				}
			}
		case statusCompleted:
			{
				log.Println("Removing the COMPLETED task :", task.Id, "  TASK :", task)
				dequeue(q)
			}
		case statusTimeout:
			{
				log.Println("Removing the TIMEDOUT task  :", task.Id, "  TASK ", task)
				dequeue(q)
			}
		}

		q.mu.Unlock()
		if len(queue.tasks) == 0 {
			break
		}
	}
	log.Println("Cleaner Exited")

}
func (q *Queue) Push(task Task) *Queue {
	q.tasks = append(q.tasks, task)
	return q
}

func (q *Queue) Pop() *Queue {
	q.tasks = q.tasks[1:]
	return q
}

func enqueue(g QueueOperation, task Task) {
	g.Push(task)
}
func dequeue(g QueueOperation) {
	g.Pop()
}
