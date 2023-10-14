package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime
		// print("\nCurrent gantt append: ProcessID: ", processes[i].ProcessID, ", WaitingTime: ", waitingTime, ", start: ", start, ", stop: ", serviceTime, ", Burst Duration: ", processes[i].BurstDuration, "\n")

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// a basic removal funciton in order to remove the identified process by its ProcessID
// I used the processID itself instead of the process as a whole as in some of the functions 
// change values in the process making the process itself a different variable when searched for
func remove(s []Process, j int64) []Process {
    var k int64		// identifier for position
	for i := range s{		// search the slice for the identifiable process
		if s[i].ProcessID == j{		// if the id matches
			k = int64(i)		// the k value holds the position
			break		// leave the search
		}
	}
	s[k] = s[len(s)-1]		// adjust the slice to ensure the slice no longer contains the process
    return s[:len(s)-1]		// return the adjusted slice value so that the slice is no longer inside the slice
}

// basic find matching process and its position in the slice function
func find(s []Process, i int64) int64 {
	for j:=0; j<len(s); j++{		// search the slice
		if s[j].ProcessID == i{		// if the current process matches the process id of the intended target
			return int64(j)			// return the position in which the process is found in the slice
		}
	}
	return 0						// if there is no matching position the value 0 is returned as a control return
}

// function to find how many times a process is identified in a given slice
func findRep(s []Process, i int64) int64 {
	var k int64 = 0					// variable counting how many times a process is identified
	for j := range s {				// search the whole slice
		if s[j].ProcessID == i {			// if the current process has the same process id
			k++						// increment k to match the count of matching processes
		}
	}
	return k						// return the count of matching processes
}

// search function used to search if a process is present in a process slice
func searchP(slic []Process, Pr Process) bool{
	for a := range slic{		// search the slice for the process one by one
		if slic[a] == Pr {		// if the current process is the same
			return true			// return true, indicating the process is present in the given slice
		}
	}
	return false				// if the process was not identified, return false suggesting no match was found
}

// Shortest Job First schedule function
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		low				int64 = 100
		change			bool = false
		currentProc		int64 = 0
		lengthP 		int64 = int64(len(processes)) + int64(1)
		starts			= make([]int64, 100)
		schedule        = make([][]string, lengthP)
		gantt           = make([]TimeSlice, 0)
		arrived			= make([]Process, len(processes))
		inProgress 		Process
		templateP		Process
		ran				= make([]Process, len(processes))
	)
	inProgress.ProcessID = -1		// placeholder process variables are loaded with identifiers to ensure they are not mis-adjusted
	templateP.ProcessID = -1
	templateP.BurstDuration = 100

	for i := 0; i<=99; i++ {	
		low = 100
		change = false
		for a := range processes{
			if processes[a].ArrivalTime <= int64(i) && !searchP(ran, processes[a]){
				arrived = append(arrived, processes[a])
			}
		}
		if inProgress.ProcessID >= 0 && !searchP(ran, inProgress){
			low = inProgress.BurstDuration
		}
		for b := range arrived {
			if b < len(arrived){
				if arrived[b].BurstDuration < low && !searchP(ran, arrived[b]){
					inProgress = arrived[b]
					change = true
				}
			}
		}
		if change == true {
			starts[inProgress.ProcessID] = int64(i)
		}
		if inProgress.ProcessID >= 0 && !searchP(ran, inProgress){
			if int64(i) == inProgress.BurstDuration + starts[inProgress.ProcessID] {
				currentProc += 1
				waitingTime = starts[inProgress.ProcessID] - inProgress.ArrivalTime
				totalWait += float64(waitingTime)
				start := waitingTime + inProgress.ArrivalTime		// burst + start - arrival
				turnaround := inProgress.BurstDuration + waitingTime
				totalTurnaround += float64(turnaround)
				completion := inProgress.BurstDuration + inProgress.ArrivalTime + waitingTime
				lastCompletion = float64(completion)
				schedule[currentProc] = []string{
					fmt.Sprint(inProgress.ProcessID),
					fmt.Sprint(inProgress.Priority),
					fmt.Sprint(inProgress.BurstDuration),
					fmt.Sprint(inProgress.ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}
				serviceTime = int64(i)
				gantt = append(gantt, TimeSlice{
					PID:   inProgress.ProcessID,
					Start: start,
					Stop:  serviceTime,
				})
				ran = append(ran, inProgress)
				inProgress = templateP
				for b := range arrived {
					if b < len(arrived){
						if arrived[b].BurstDuration < low && !searchP(ran, arrived[b]){
							inProgress = arrived[b]
							change = true
						}
					}
				}
				if change == true {
					starts[inProgress.ProcessID] = int64(i)
				}
			}
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}


func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		low				int64 = 100
		change			bool = false
		currentProc		int64 = 0
		lengthP 		int64 = int64(len(processes)) + int64(1)
		starts			= make([]int64, 100)
		schedule        = make([][]string, lengthP)
		gantt           = make([]TimeSlice, 0)
		arrived			= make([]Process, len(processes))
		inProgress 		Process
		templateP		Process
		ran				= make([]Process, len(processes))
	)
	inProgress.ProcessID = -1
	templateP.ProcessID = -1
	templateP.BurstDuration = 100

	for i := 0; i<=99; i++ {	
		low = 100
		change = false
		for a := range processes{
			if processes[a].ArrivalTime <= int64(i) && !searchP(ran, processes[a]){
				arrived = append(arrived, processes[a])
			}
		}
		if inProgress.ProcessID >= 0 && !searchP(ran, inProgress){
			low = inProgress.BurstDuration
		}
		for b := range arrived {		// adjust this loop to include the priority property and split ties based on priority
			if b < len(arrived){
				if arrived[b].BurstDuration == low && !searchP(ran, arrived[b]){
					if arrived[b].Priority < inProgress.Priority{
						inProgress = arrived[b]
					} else {
						continue
					}
				}
				if arrived[b].BurstDuration < low && !searchP(ran, arrived[b]){
					inProgress = arrived[b]
					change = true
				}
			}
		}
		if change == true {
			starts[inProgress.ProcessID] = int64(i)
		}
		if inProgress.ProcessID >= 0 && !searchP(ran, inProgress){
			if int64(i) == inProgress.BurstDuration + starts[inProgress.ProcessID] {
				currentProc += 1
				waitingTime = starts[inProgress.ProcessID] - inProgress.ArrivalTime
				totalWait += float64(waitingTime)
				start := waitingTime + inProgress.ArrivalTime		// burst + start - arrival
				turnaround := inProgress.BurstDuration + waitingTime
				totalTurnaround += float64(turnaround)
				completion := inProgress.BurstDuration + inProgress.ArrivalTime + waitingTime
				lastCompletion = float64(completion)
				schedule[currentProc] = []string{
					fmt.Sprint(inProgress.ProcessID),
					fmt.Sprint(inProgress.Priority),
					fmt.Sprint(inProgress.BurstDuration),
					fmt.Sprint(inProgress.ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}
				serviceTime = int64(i)
				gantt = append(gantt, TimeSlice{
					PID:   inProgress.ProcessID,
					Start: start,
					Stop:  serviceTime,
				})
				ran = append(ran, inProgress)
				inProgress = templateP
				for b := range arrived {
					if b < len(arrived){
						if arrived[b].BurstDuration < low && !searchP(ran, arrived[b]){
							inProgress = arrived[b]
							change = true
						}
					}
				}
				if change == true {
					starts[inProgress.ProcessID] = int64(i)
				}
			}
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// writing the Round-Robin scheduling function, I copied the basis of the First Come First Serve function
// the FCFS function and Round Robin are essentially the same in so far as handling the processes in arrival order
// the main difference is programming in a timer variable to pre-empt the processes which do not finish in the allotted time
func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		maxTime			int64 = 1
		secondQ			= make([]Process, 0)
		temp1			Process
		lstRan			= make([]Process, len(processes))
		starts			= make([]int64, 100)
		currentProc		int64 = -1
	)
	for z := range starts{			// initializes the starts queue with -1 as a start time so that -1 can be used as a blank flag
		starts[z] = -1				// as no start time would be -1
	}
	for a := 0; a<100; a++{			// a = seconds and is therefore the timer for the function
		for b := range processes{
			if processes[b].ArrivalTime <= int64(a) && findRep(lstRan, processes[b].ProcessID) == 0 && findRep(secondQ, processes[b].ProcessID) == 0{
				secondQ = append(secondQ, processes[b])
			} else {
				continue
			}
		}
		if len(secondQ) != 0 {
			if secondQ[0].ProcessID == 0{
				secondQ = remove(secondQ, 0)
			}
			if starts[secondQ[0].ProcessID] == -1 {
				starts[secondQ[0].ProcessID] = int64(a)
			}
			secondQ[0].BurstDuration -= int64(maxTime)
			temp1 = secondQ[0]
			gantt = append(gantt, TimeSlice{
				PID:   temp1.ProcessID,
				Start: int64(a),
				Stop:  serviceTime,
			})
			if secondQ[0].BurstDuration == 0 {
				lstRan = append(lstRan, secondQ[0])
				secondQ = remove(secondQ, secondQ[0].ProcessID)
				currentProc += 1
				waitingTime = starts[temp1.ProcessID] - temp1.ArrivalTime
				totalWait += float64(waitingTime)
				turnaround := int64(a) + waitingTime
				totalTurnaround += float64(turnaround)
				completion := int64(a)
				lastCompletion = float64(completion)
				var temp2 int64 = 0
				for d := range processes {
					if processes[d].ProcessID == temp1.ProcessID {
						temp2 = int64(d)
						break
					}
				}
				schedule[currentProc] = []string{
					fmt.Sprint(temp1.ProcessID),
					fmt.Sprint(temp1.Priority),
					fmt.Sprint(processes[temp2].BurstDuration),
					fmt.Sprint(temp1.ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}
				serviceTime = int64(a)
			} else{
				secondQ = remove(secondQ, temp1.ProcessID)
				secondQ = append(secondQ, temp1)
				continue
			}
		}
	} 
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region Output helpers

/*func sameProcess(slice1, slice2 []string) bool {
    for i := range slice1 {
        if slice1[0] != slice2[0][0] {
            return false
        }
    }
    return true
}*/

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
