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


func remove(s []Process, j int64) []Process {
    var k int64
	for i := range s{
		if s[i].ProcessID == j{
			k = int64(i)
			break
		}
	}
	s[k] = s[len(s)-1]
    return s[:len(s)-1]
}

func find(s []Process, i int64) int64 {
	for j:=0; j<len(s); j++{
		if s[j].ProcessID == i{
			return int64(j)
		}
	}
	return 0
}

func findRep(s []Process, i int64) int64 {
	var k int64 = 0
	for j := range s {
		if s[j].ProcessID == i {
			k++
		}
	}
	return k
}

func newProcess(ID int64) *Process {
	run1 := Process{ProcessID: ID}
	return &run1
}

func searchP(slic []Process, Pr Process) bool{
	for a := range slic{
		if slic[a] == Pr {
			return true
		}
	}
	return false
}

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
	inProgress.ProcessID = -1
	templateP.ProcessID = -1
	templateP.BurstDuration = 100

	for i := 0; i<=99; i++ {	
		low = 100
		/*if i <= 20 {
			print("Time is: ", i, "\n")
		}*/ 
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
				// print("time is currently: ", i, " = ", inProgress.BurstDuration, " + ", starts[inProgress.ProcessID])
				// print("starts: ", starts[inProgress.ProcessID], " - ArrivalTime: ", inProgress.ArrivalTime, " = WaitingTime: ", waitingTime, "\n")
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
				// print("\nCurrent gantt append: ProcessID: ", inProgress.ProcessID, ", Waiting time: ", waitingTime, ", start: ", start, ", stop: ", serviceTime, ", Burst Duration: ", inProgress.BurstDuration, "\n")
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
				// print("time is currently: ", i, " = ", inProgress.BurstDuration, " + ", starts[inProgress.ProcessID])
				// print("starts: ", starts[inProgress.ProcessID], " - ArrivalTime: ", inProgress.ArrivalTime, " = WaitingTime: ", waitingTime, "\n")
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
				// print("\nCurrent gantt append: ProcessID: ", inProgress.ProcessID, ", Waiting time: ", waitingTime, ", start: ", start, ", stop: ", serviceTime, ", Burst Duration: ", inProgress.BurstDuration, "\n")
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
		// boo1			bool = false
		// secondQ			= make([]Process, len(processes))
		status			= make([]bool, len(processes))
	)
	for a := range processes {
		if processes[a].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[a].ArrivalTime
		}
		totalWait += float64(waitingTime)
		start := waitingTime + processes[a].ArrivalTime
		// print("\nCurrent gantt append: ProcessID: ", processes[a].ProcessID, ", WaitingTime: ", waitingTime, ", start: ", start, ", stop: ", serviceTime, ", Burst Duration: ", processes[i].BurstDuration, "\n")
		if maxTime >= processes[a].BurstDuration {
			status[a] = true
			turnaround := maxTime + waitingTime
			totalTurnaround += float64(turnaround)
			completion := maxTime + processes[a].ArrivalTime + waitingTime
			lastCompletion = float64(completion)
			schedule[a] = []string{
				fmt.Sprint(processes[a].ProcessID),
				fmt.Sprint(processes[a].Priority),
				fmt.Sprint(processes[a].BurstDuration),
				fmt.Sprint(processes[a].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}
			serviceTime += maxTime
			gantt = append(gantt, TimeSlice{
				PID:   processes[a].ProcessID,
				Start: start,
				Stop:  serviceTime,
			})
		} else {
			status[a] = false
			/*serviceTime += maxTime
			gantt = append(gantt, TimeSlice{
				PID:   processes[a].ProcessID,
				Start: start,
				Stop:  serviceTime,
			})*/
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
