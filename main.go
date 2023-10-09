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
	
	//SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	//RRSchedule(os.Stdout, "Round-robin", processes)
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

func SJFSchedule(w io.Writer, title string, processes []Process){			// needs to give avg turn, avg wait, and avg throughput
	// var low int64 = 100
	var (
		// lengthP int = (len(processes) + 1)
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	starts := make(map[int64]int64)
	ran := []Process{}
	var completion int64 = 0
	var start int64 = 0
	var turnaround int64 = 0
	var currentP int64 = 0
	completion = start + completion + turnaround
	var temp1 int64
	// slic := []Process{}
	runnin := []Process{}
	var run1 Process
	// var active bool = false
	var lowest int64 = 1000
	// run1.ProcessID = -1			// first choice to run placeholder
	for i := 0; i<=99; i++ {		// time interval for loop, cycles from 0-99 for each second
		// var lowest int64 = 1000		// initializes the lowest burst placeholder with a very high value so that it is not automatically the quickest on the stack
		var temp2 int64
		// var boo bool = false
		if i <= 20 {
			print("time is: ", i, "runnin list is: ")
			for r := range runnin {
				print(runnin[r].ProcessID, ", ")
			}
			print("\n")
			print("time is: ", i, "ran list is: ")
			for t := range ran {
				print(ran[t].ProcessID, ", ")
			}
			print("\n")
			print("time is: ", i, "gantt list is: ")
			for w := range gantt {
				print(gantt[w].PID, ", ")
			}
			print("\n")
		}
		if len(runnin) > 0 {
			for p := range runnin {
				if p < len(runnin) {
					temp2 = runnin[p].ProcessID
					if findRep(runnin, temp2) > 1 {
						runnin = remove(runnin, temp2)
					}
				}
			}
		}
		for j := range processes{	// for loop to now cycle through the processes and decide if a new process needs to be started
			if processes[j].ArrivalTime <= int64(i) {
				if findRep(ran, processes[j].ProcessID) == 0 {
					if processes[j].BurstDuration < lowest {
						lowest = processes[j].BurstDuration
					}
					if findRep(runnin, processes[j].ProcessID) == 0{
						runnin = append(runnin, processes[j])
					}
				}
				// 1111111112
				// print(processes[j].ProcessID)
				// print(runnin[len(runnin)-1].ProcessID)
				// var run1 Process
				// run1 is now run and therefore needs to be "run" so make it do all the stats stuff.
			} else {
				continue
			}	// line 92 starts this section in FCFS function
		}
		if len(runnin) == 0{
			continue
		}
		for k := range runnin {
			if runnin[k].BurstDuration < run1.BurstDuration {
				run1 := runnin[k]
				starts[run1.ProcessID] = int64(i)
			} else {
				continue
			}
		}
		if run1.BurstDuration == (int64(i) - starts[run1.ProcessID]) {
			// var curP []string
			// var temp3 []string
			for l := range runnin {
				if l >= 0 && l < len(runnin) && runnin[l] == run1 {
					if len(runnin) > 1 {
						runnin = remove(runnin, int64(l))
					} else {
						runnin = []Process{}
					}
				}
			}
			for m := range processes{
				if processes[m] == run1 {
					temp1 = int64(m)
				}
			}
			// currentP += 1
			/*for t := range ran {
				if ran[t] == run1 {
					boo = true
				}
			}
			if boo == true{
				continue
			}*/
			/*curP = []string{
				fmt.Sprint(processes[temp1].ProcessID),
				fmt.Sprint(processes[temp1].Priority),
				fmt.Sprint(processes[temp1].BurstDuration),
				fmt.Sprint(processes[temp1].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}
			for u := range schedule {
				temp3 = schedule[u]
				if temp3 == curP {
					boo = true
				}
			}*/

			/*for s := range schedule {
				if curP[0] != schedule[s][0] {
					boo = false
				}
			}

			if boo == true {
				continue
			}*/
			// |  1 |        2 |     5 |       0 |       0 |          5 |          5 |

			waitingTime = serviceTime - processes[temp1].ArrivalTime
			totalWait += float64(waitingTime)
			start := waitingTime + processes[temp1].ArrivalTime
			turnaround := processes[temp1].BurstDuration + waitingTime
			totalTurnaround += float64(turnaround)
			completion := processes[temp1].BurstDuration + processes[temp1].ArrivalTime + waitingTime
			lastCompletion = float64(completion)
			var boo bool = false
			if currentP < int64(len(processes)) && len(schedule[0]) > 0{
				if len(schedule) > 0 {
					for x := range schedule {
						if schedule[x][0] != string(temp1) {
							continue
						} else {
							boo = true
						}
					}
					if boo == false {
						schedule[currentP] = []string{
							fmt.Sprint(processes[temp1].ProcessID),
							fmt.Sprint(processes[temp1].Priority),
							fmt.Sprint(processes[temp1].BurstDuration),
							fmt.Sprint(processes[temp1].ArrivalTime),
							fmt.Sprint(waitingTime),
							fmt.Sprint(turnaround),
							fmt.Sprint(completion),
						}
						currentP += 1
					}
				}
				// print("currentP is: ", schedule[currentP-1], "\n")
			}
			serviceTime += processes[temp1].BurstDuration
			// print("Schedule currentP processID is: ", schedule[currentP], "\n")
			/*var boo bool = false
			for v := range gantt {
				if gantt[v].PID == temp1 {
					boo = true
				}
			}
			if boo != true {
				gantt = append(gantt, TimeSlice{
					PID:   processes[temp1].ProcessID,
					Start: start,
					Stop:  serviceTime,
				})
			}*/
			gantt = append(gantt, TimeSlice{
				PID:   processes[temp1].ProcessID,
				Start: start,
				Stop:  serviceTime,
			})
			lowest = 1000
			for n := range runnin {
				if runnin[n].BurstDuration < lowest {
					run1 = runnin[n]
					lowest = runnin[n].BurstDuration
				}
			}
			ran = append(ran, run1)
			continue
		}
		
		
		
		// adjust here so that the stuff happening is either above /\ where it happens EACH TIME a process completes
		// or happens below \/ after ALL processes complete
		/*for o := range runnin {
			// print(runnin[o].ProcessID)
			print("Value of i: ", i, ", ")
    		print("Value of o: ", runnin[o].ProcessID)
			print("\n")
		}
		for q := range ran {
			print("ran: ", ran[q].ProcessID)
			print("\n")
		}*/
	}
	

	
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}
//func SJFSchedule(w io.Writer, title string, processes []Process) { }
//
//func RRSchedule(w io.Writer, title string, processes []Process) { }

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
