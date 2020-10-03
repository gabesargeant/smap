package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"smap/record"
	"strconv"
	"strings"
)

// Args  about
type Args struct {
	CSV     *string
	OutDir  *string
	Verbose *bool
}

// Verbose printing of comments.
var Verbose bool

//
func main() {

	args := defineFlags()

	flag.Parse()
	Verbose = *args.Verbose

	if args.CSV != nil && *args.CSV != "" {
		if Verbose == true {
			fmt.Println("Path Supplied for location of one CSV")
		}

		f := findFiles(*args.CSV)
		createOutDir(*args.OutDir)
		outfile := createOutFile(*args.OutDir, *args.CSV)
		readCSV(f, outfile)

	} else {

		fmt.Println("no path supplied for CSV option")
		flag.PrintDefaults()
		os.Exit(99)
	}

}

func findFiles(filepath string) *os.File {
	if Verbose == true {
		fmt.Printf("attempting to get %s \n", filepath)
	}
	f, err := os.Open(filepath)

	check(err)

	return f

}

func createOutDir(outDir string) {

	err := os.Mkdir(outDir, 0777)
	if os.IsNotExist(err) {
		check(err)
	}
}

func createOutFile(outDir string, inputCSVPath string) *os.File {

	name := filepath.Base(inputCSVPath)
	name += ".json"

	outName := filepath.Join(outDir, name)
	outfile, err := os.Create(outName)
	//(name, os.O_RDONLY|os.O_CREATE, 0666)
	check(err)

	return outfile
}

//Check error used for places where a panic is more apt than dealing with the error
func check(e error) {
	if e != nil {
		panic(e)
	}
}

func readCSV(file *os.File, outFile *os.File) {
	if Verbose == true {
		fmt.Println("reading CSV")
	}

	br := bufio.NewReader(file)
	r := csv.NewReader(br)

	firstLine, _ := r.Read()
	if Verbose == true {
		fmt.Println(firstLine[1:])
	}
	bw := bufio.NewWriter(outFile)
	//Write Start Array
	writeOutFile([]byte("["), bw)

	var buff []byte             //the json object
	var buffCommaNewLine []byte //the json object with a ,\n on it

	partitionID := extractTableName(file)
	geoLevel := extractGeoLevel(file)

	for {

		row, err := r.Read()
		if err == io.EOF {
			//for the last record, write the buffer without the commaNewLine
			writeOutFile(buff, bw)
			break
		}

		//write output from the previous iteration, first write is empty
		writeOutFile(buffCommaNewLine, bw)

		if err != nil {
			log.Fatal(err)
		}

		buff = record.BuildRecord(row[0], partitionID, geoLevel, buildJSONMaps(firstLine[1:], row[1:]))
		buffCommaNewLine = append(buff, ","...)

	}
	//write end bracket
	writeOutFile([]byte("]"), bw)

}

func extractTableName(file *os.File) string {

	name := filepath.Base(file.Name())

	s := strings.Split(name, "_")

	return s[1]

}

func extractGeoLevel(file *os.File) string {

	name := filepath.Base(file.Name())
	fmt.Println(name)
	s := strings.Split(name, "_")
	t := strings.Split(s[len(s)-1], ".")
	return t[0]

}

func writeOutFile(bytes []byte, bufferedWriter *bufio.Writer) {

	bufferedWriter.Write(bytes)

	bufferedWriter.Flush()

}

func buildJSONMaps(keys []string, values []string) map[string]float64 {

	data := make(map[string]float64)

	for i := 0; i < len(keys); i++ {
		if values[i] == ".." {
			values[i] = "-1"
		}
		j, err := strconv.ParseFloat(values[i], 64)
		check(err)
		data[keys[i]] = j
	}

	//fmt.Println(data)

	return data

}

func defineFlags() Args {
	var a = Args{}
	///home/gabe/Documents/census/2016_GCP_ALL_for_AUS_short-header/2016 Census GCP All Geographies for AUST/STE/AUST/2016Census_G02_AUS_STE.csv
	a.CSV = flag.String("c", "/home/gabe/Documents/census/2016_GCP_ALL_for_AUS_short-header/2016-Census-GCP-All-Geography-for-AUST/STE/AUST/2016Census_G02_AUS_STE.csv", "CSV Location: a path to a single file")
	a.OutDir = flag.String("o", "./out_json", "Output Directory, if not specified all outputs will be written to ./out")
	a.Verbose = flag.Bool("v", false, "Run loudly, default false")

	return a
}
