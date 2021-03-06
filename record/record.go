package record

import (
	"encoding/json"
	"io/ioutil"
)

// Record a struct used to create a json object representing a region ID and then a set of key value pairs of data.
type Record struct {
	RegionID    string             `json:"RegionID"`
	GeoLevel    string             `json:"GeoLevel"`
	PartitionID string             `json:"PartitionID"` //TODO change this to partionID
	KVPairs     map[string]float64 `json:"KVPairs"`
}

// Record a struct used to create a json object representing a region ID and then a set of key value pairs of data.
type DeleteRecord struct {
	RegionID    string `json:"RegionID"`
	PartitionID string `json:"PartitionID"`
}

//Collection Array of Records
type Collection struct {
	ArrRecord []Record
}

//NewRecordArray returns a struct that holds an array of records
func NewRecordArray() Collection {

	ar := Collection{}

	return ar

}

//BuildRecord .. of Map into Json object.
func BuildRecord(rec string, partitionID string, geoLevel string, jmap map[string]float64) []byte {

	r := Record{}

	r.RegionID = rec
	r.KVPairs = jmap
	r.PartitionID = partitionID
	r.GeoLevel = geoLevel

	b, err := json.Marshal(r)

	check(err)

	return b
}

func check(err error) {
	if err != nil {
		panic(err.Error)
	}
}

//OpenRecordsAtPath open a json object and marshal it.
func OpenRecordsAtPath(path string) []Record {

	file, err := ioutil.ReadFile(path)

	var col = []Record{}

	err = json.Unmarshal([]byte(file), &col)

	check(err)

	return col

}

// CreateDeleteRecord Returns a range of DeleteRecords that
// are records featuring primary keys for the delete opperation.
func CreateDeleteRecord(record []Record) []DeleteRecord {

	var rtnRecs = []DeleteRecord{}

	for _, r := range record {

		dr := DeleteRecord{}
		dr.RegionID = r.RegionID
		dr.PartitionID = r.PartitionID

		rtnRecs = append(rtnRecs, dr)
	}

	return rtnRecs
}
