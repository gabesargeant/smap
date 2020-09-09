# Serverless Mapping Project
---

## csvtransform
This package contains go code that consumes a csv file and outputs and array of json objects. 
The output objects look like this and represent 1 row from the CSV file.
```
// Record a struct used to create a json object representing a region ID and then a set of key value pairs of data.
type Record struct {
	RegionID string             `json:"RegionID"`
	TableID  string             `json:"TableID"`
	KVPairs  map[string]float64 `json:"KVPairs"`
}
```
Important points, The region Id links to some geography area. So it should be a 'joinable' code.

## dbbuilder
This package contains go code to build a dynamodb table in fill it with the outputs of csvtransform. 
It interacts with the exported Record Package.

## Record. 
Makes the above listed structs




