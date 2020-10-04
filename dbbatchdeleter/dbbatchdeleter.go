package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"smap/record"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Args CMD Line ARgs
type Args struct {
	DynamoDBTableName *string
	InputFile         *string
	DeleteData        *bool
}

// DryRun - don't remove anything
var DryRun bool = true

// Main - Entry point for writing files into a DynamoDB
func main() {
	fmt.Println("Starting DB Writer")

	args := defineFlags()
	flag.Parse()

	if *args.DynamoDBTableName == "" || *args.InputFile == "" {
		flag.Usage()
		os.Exit(-99)
	}
	fmt.Println("delete dir and confirm dir")
	fmt.Println(*args.DeleteData)

	if *args.DeleteData != true {
		fmt.Println("Select Confirm Delete, and Delete Data")
		fmt.Println("**Dry - Run ** ")
		DryRun = true
	} else {
		fmt.Println("** The REAL Deal - DROPPING RECORDS NOW ** ")
		DryRun = false
	}

	sess := establishAWSSession()

	fmt.Print("Opening Record: ")

	fmt.Println(filepath.Base(*args.InputFile))
	recs := record.OpenRecordsAtPath(*args.InputFile)
	drecs := record.CreateDeleteRecord(recs)

	if *args.DeleteData {
		removeThisData(sess, *args.DynamoDBTableName, &drecs)
	}

}

func removeThisData(sess *session.Session, name string, recs *[]record.DeleteRecord) {

	svc := dynamodb.New(sess)

	batchWriteItemDeletes := composeBatchInputs(recs, name)

	outArr := []*dynamodb.BatchWriteItemOutput{}

	if DryRun {
		fmt.Println("Select Confirm Delete, and Delete Data")
		fmt.Println("**Dry - Run ** ")
		for _, batchReq := range *batchWriteItemDeletes {

			fmt.Println(batchReq)

		}

		os.Exit(0)
	}

	for _, batchReq := range *batchWriteItemDeletes {

		result, err := svc.BatchWriteItem(&batchReq)

		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeProvisionedThroughputExceededException:
					fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
				case dynamodb.ErrCodeResourceNotFoundException:
					fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
				case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
					fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
				case dynamodb.ErrCodeRequestLimitExceeded:
					fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
				case dynamodb.ErrCodeInternalServerError:
					fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}

		}
		outArr = append(outArr, result)
	}

	printAllResults(outArr)
}

func printAllResults(outArr []*dynamodb.BatchWriteItemOutput) {

	for _, result := range outArr {
		if len(result.UnprocessedItems) != 0 {
			fmt.Println(*result)
		}

	}
}

func composeBatchInputs(recs *[]record.DeleteRecord, name string) *[]dynamodb.BatchWriteItemInput {

	buckets := (len(*recs) / 25) + 1
	fmt.Print("Number of Buckets: ")
	fmt.Println(buckets)
	fmt.Print("Number of Records: ")
	fmt.Println(len(*recs))
	arrayBatchRequest := make([]dynamodb.BatchWriteItemInput, buckets)

	for i := 0; i < buckets; i++ {

		wrArr := []*dynamodb.WriteRequest{}

		stepValue := i * 25

		for j := 0; j < 25; j++ {

			if j+stepValue == len(*recs) {

				break
			}

			av, err := dynamodbattribute.MarshalMap((*recs)[j+stepValue])

			if err != nil {
				fmt.Println("Got Error unmarshalling map")
				fmt.Println((*recs)[i*j])
				fmt.Print("Loop ", i, j)
				fmt.Println(err.Error())
				os.Exit(1)
			}

			dr := dynamodb.DeleteRequest{}

			dr.SetKey(av)
			//fmt.Println(dr)
			wr := dynamodb.WriteRequest{}
			wr.SetDeleteRequest(&dr)

			wrArr = append(wrArr, &wr)

		}
		wrMap := make(map[string][]*dynamodb.WriteRequest, 1)

		wrMap[name] = wrArr

		arrayBatchRequest[i].SetRequestItems(wrMap)

	}
	return &arrayBatchRequest
}

func establishAWSSession() *session.Session {

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		//SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{Region: aws.String("ap-southeast-2")},
	}))

	return sess
}

func getReader(inputFile string) io.Reader {

	file, err := os.Open(inputFile)
	check(err)
	reader := bufio.NewReader(file)

	return reader

}

func check(err error) {
	if err != nil {
		panic(err)
	}

}
func defineFlags() Args {

	var args = Args{}
	args.DynamoDBTableName = flag.String("n", "test", "Name of the DynamoDB Table")
	args.InputFile = flag.String("f", "", "File to read, expected json format")
	args.DeleteData = flag.Bool("d", false, "delete data specified in json file at location -f .")
	return args
}
