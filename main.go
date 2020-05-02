package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup

func isDivider(a uint8) bool {
	// equal to regex /[^a-zA-Z0-9]/ true for non-alphanumeric characters
	return !((a > 64 && a < 90) || (a > 96 && a < 123) || (a > 48 && a < 57))
}

func formatTime(timeNs int64) string {
	timeMs := timeNs / 1000000
	sec := float64(timeNs / 1000000000)
	min := math.Floor(sec / 60)
	sec = sec - min*60
	if min > 0 {
		return fmt.Sprintf("%dms (%.0fm %.0fs)", timeMs, min, sec)
	}
	if sec > 0 {
		return fmt.Sprintf("%dms (%.0fs)", timeMs, sec)
	}
	return fmt.Sprintf("%dms", timeMs)
}

func pluralAppendS(data int) string {
	if data > 1 {
		return "s"
	}
	return ""
}

func sortArray(array *[]string, wordCount map[string]int) {
	defer wg.Done()
	var maximumIndex int
	var tempElement string
	for i := 0; i < len(*array)-1; i++ {
		maximumIndex = i
		for j := i + 1; j < len(*array); j++ {
			if wordCount[(*array)[j]] > wordCount[(*array)[maximumIndex]] {
				maximumIndex = j
			}
		}
		tempElement = (*array)[maximumIndex]
		(*array)[maximumIndex] = (*array)[i]
		(*array)[i] = tempElement
	}
}

func countOccurencies(text string, channel chan map[string]int) {
	defer wg.Done()
	var spaces = 0
	var start, end int
	var lowerCase string
	var wordCounts = make(map[string]int)
	for i := 1; i < len(text)-1; i++ {
		if isDivider(text[i]) {
			i++ // in case there is a space after comma or period
			spaces++
		}
	}
	for i := 0; i < len(text); i++ {
		if text[i] > 64 && text[i] < 91 {
			lowerCase += string(text[i] + 32)
		} else {
			lowerCase += string(text[i])
		}
	}
	var words = make([]string, 0, spaces+1)
	for i := 0; i < len(lowerCase); i++ {
		if i == 0 && isDivider(lowerCase[i]) {
			start = i
		} else if isDivider(lowerCase[i]) && (i+1 == len(lowerCase) || !isDivider(lowerCase[i+1])) {
			start = i + 1
		} else if !isDivider(lowerCase[i]) && (i+1 == len(lowerCase) || isDivider(lowerCase[i+1])) {
			end = i + 1
			if end == len(lowerCase) {
				words = append(words, lowerCase[start:])
			} else {
				words = append(words, lowerCase[start:end])
			}
		}
	}
	for i := 0; i < len(words); i++ {
		wordCounts[words[i]] = wordCounts[words[i]] + 1
	}
	wordCounts["words_length"] = len(words)
	channel <- wordCounts
}

func printHelp() {
	fmt.Printf("Hello to large text processing script!!!\nOutput of this script will show the most occuring words in your text.\nIt ignores case of letters.")
	fmt.Printf("Possible arguments:\n\n-f\tUse to define text file path\n")
	fmt.Printf("-p\tDefine count of threads to speed up processing\n")
	fmt.Printf("-v\tVerbose mode, more info about processing\n")
	fmt.Printf("-m\tChange ranking count, default = 5\n")
	fmt.Printf("-t\tIf file path not used, use this to define text \n")
	fmt.Printf("\tUse \"...\" to wrap text or use -t as last parameter\n")
}

func processArgs() (bool, string, bool, bool, int, int) {
	var verbose, showTimes = false, false
	var text, path string
	var numOfThreads, rankingCount = 1, 5
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--help" {
			printHelp()
			return true, text, verbose, showTimes, numOfThreads, rankingCount
		}
		switch os.Args[i] {
		case "-v":
			verbose = true
		case "-s":
			showTimes = true
		}
		if i < len(os.Args)-1 {
			switch os.Args[i] {
			case "-t":
				text = strings.Join(os.Args[i+1:], " ")
				break
			case "-f":
				path = os.Args[i+1]
				if len(path) > 0 {
					data, err := ioutil.ReadFile(path)
					if err != nil {
						fmt.Println(err.Error())
						return true, text, verbose, showTimes, numOfThreads, rankingCount
					}
					text = string(data)
					if len(text) == 0 {
						fmt.Println("No text in the file")
						return true, text, verbose, showTimes, numOfThreads, rankingCount
					}
				}
			case "-p":
				numOfThreads, _ = strconv.Atoi(os.Args[i+1])
			case "-m":
				rankingCount, _ = strconv.Atoi(os.Args[i+1])
			}
		}
	}
	if len(text) == 0 {
		fmt.Println("No text provided.\nUse -f to provide a filepath or -t to provide a text")
		return true, text, verbose, showTimes, numOfThreads, rankingCount
	}
	fmt.Printf("\nProcessing text%s\nUsing %d thread%s\n\n", (func() string {
		if len(path) > 0 {
			return fmt.Sprintf(" file '%s'.", path)
		}
		return "."
	})(), numOfThreads, pluralAppendS(numOfThreads))
	return false, text, verbose, showTimes, numOfThreads, rankingCount
}

func main() {
	var textLength, partLength, pivot, lastPivot, allWordsLength int
	var startTime int64
	var totalWordCounts = make(map[string]int)
	var endNow, text, verbose, showTimes, numOfThreads, rankingCount = processArgs()
	if endNow {
		return
	}
	var wordCountsChannel = make(chan map[string]int, numOfThreads)
	textLength = len(text)
	if verbose {
		fmt.Printf("Text length: %d\n", textLength)
	}
	startTime = time.Now().UnixNano()
	partLength = textLength / numOfThreads
	wg.Add(numOfThreads)
	for i := 0; i < numOfThreads; i++ {
		// finding non-alphanumeric character near the the splitting point to prevent word breaking.
		pivot = (i + 1) * partLength
		for pivot < len(text) && !isDivider(text[pivot]) {
			pivot++
		}
		if pivot < lastPivot {
			fmt.Println("You are probably using more threads then there are words")
			return
		}
		if numOfThreads == 1 {
			go countOccurencies(text, wordCountsChannel)
		} else if i == 0 {
			go countOccurencies(text[:pivot], wordCountsChannel)
		} else if i == numOfThreads-1 {
			go countOccurencies(text[lastPivot:], wordCountsChannel)
		} else {
			go countOccurencies(text[lastPivot:pivot], wordCountsChannel)
		}
		lastPivot = pivot + 1
	}
	wg.Wait()
	for i := 0; i < numOfThreads; i++ {
		wordCount, ok := <-wordCountsChannel
		if verbose {
			fmt.Println(wordCount)
		}
		if ok {
			for word, count := range wordCount {
				if word == "words_length" {
					allWordsLength += count
				} else {
					totalWordCounts[word] = totalWordCounts[word] + count
				}
			}
		}
	}
	if showTimes {
		fmt.Printf("\nCounting words took: %s\n\n", formatTime(time.Now().UnixNano()-startTime))
	}
	var arrayToBeSortedByCount = make([]string, 0, allWordsLength)
	for word := range totalWordCounts {
		arrayToBeSortedByCount = append(arrayToBeSortedByCount, word)
	}
	if verbose {
		fmt.Println(totalWordCounts)
		fmt.Println(arrayToBeSortedByCount)
	}
	startTime = time.Now().UnixNano()
	wg.Add(numOfThreads)
	var n = len(arrayToBeSortedByCount)
	var part = n / numOfThreads
	var tempSortedData = make([]string, 0, n)
	var sortedData = make([]string, 0, n)
	var L, R []string
	var partArrays = make([][]string, 0, numOfThreads)
	for i := 0; i < numOfThreads; i++ {
		if numOfThreads == 1 {
			partArrays = append(partArrays, arrayToBeSortedByCount)
		} else if i == 0 {
			partArrays = append(partArrays, arrayToBeSortedByCount[:part])
		} else if i == numOfThreads-1 {
			partArrays = append(partArrays, arrayToBeSortedByCount[i*(part):])
		} else {
			partArrays = append(partArrays, arrayToBeSortedByCount[i*(part):(i+1)*(part)])
		}
		go sortArray(&partArrays[i], totalWordCounts)
	}
	wg.Wait()
	if verbose {
		fmt.Println(partArrays)
	}
	var j, k int
	sortedData = partArrays[numOfThreads-1]
	for x := 0; x < numOfThreads-1; x++ {
		j, k = 0, 0
		L, R = partArrays[x], sortedData
		for j < len(L) && k < len(R) {
			if totalWordCounts[L[j]] >= totalWordCounts[R[k]] {
				tempSortedData = append(tempSortedData, L[j])
				j++
			} else {
				tempSortedData = append(tempSortedData, R[k])
				k++
			}
		}
		if j < len(L) {
			tempSortedData = append(tempSortedData, L[j:]...)
		} else if k < len(R) {
			tempSortedData = append(tempSortedData, R[k:]...)
		}
		if verbose {
			fmt.Println(tempSortedData)
		}
		sortedData = tempSortedData
		tempSortedData = []string{}
	}
	if verbose {
		fmt.Println(sortedData)
	}
	fmt.Println("The most used words:")
	for i := 0; i < allWordsLength && i < rankingCount; i++ {
		word := sortedData[i]
		count := totalWordCounts[word]
		fmt.Printf("%d. %s with %d occurence%s\n", i+1, word, count, pluralAppendS(count))
	}
	if verbose || showTimes {
		fmt.Printf("\nSorting words took: %s\n\n", formatTime(time.Now().UnixNano()-startTime))
	}
}
