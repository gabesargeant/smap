package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"smap/record"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Args CMD Line ARgs
type Args struct {
	DynamoDBTableName *string
	InputFile         *string
	BuildTable        *bool
	LoadData          *bool
	Purge             *bool
	ConfirmPurge      *bool
}

// Main - Entry point for writing files into a DynamoDB
func main() {
	fmt.Println("Starting DB Writer app to pump record into a Dynamo DB Table")

	args := defineFlags()
	flag.Parse()

	if *args.DynamoDBTableName == "" || *args.InputFile == "" {
		flag.Usage()
		os.Exit(-99)
	}

	sess := establishAWSSession()

	if *args.Purge == true && *args.ConfirmPurge == true {
		fmt.Println("Purging the Table")
		purgeTable(sess, *args.DynamoDBTableName)
		os.Exit(2)
	}

	recs := record.OpenRecordsAtPath(*args.InputFile)

	if *args.BuildTable {
		rec := recs[0]

		ta := buildTableAttributes(rec)
		ks := buildKeySchema()
		cti := buildCreateTableInput(*args.DynamoDBTableName, ta, ks)
		createDynamoDBTable(sess, cti)

	}

	if *args.LoadData {

		loadDataToDynamoDB(sess, *args.DynamoDBTableName, &recs)
	}

}

func loadDataToDynamoDB(sess *session.Session, name string, recs *[]record.Record) {

	svc := dynamodb.New(sess)

	batchWriteItemInput := composeBatchInputs(recs, name)

	outArr := []*dynamodb.BatchWriteItemOutput{}

	for _, batchReq := range *batchWriteItemInput {

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
		if result != nil {
			fmt.Println(*result)
		}

	}
}

func composeBatchInputs(recs *[]record.Record, name string) *[]dynamodb.BatchWriteItemInput {

	buckets := (len(*recs) / 25) + 1
	fmt.Println("Number of Records " + string(buckets))
	fmt.Println(buckets)
	fmt.Println(len(*recs))
	arrayBatchRequest := make([]dynamodb.BatchWriteItemInput, buckets)

	for i := 0; i < buckets; i++ {

		//putreqarr := make([]dynamodb.PutRequest, 25)
		//wrArr := make([]*dynamodb.WriteRequest, 25)
		wrArr := []*dynamodb.WriteRequest{}
		//tmp := []*dynamodb.WriteRequest{}

		stepValue := i * 25

		for j := 0; j < 25; j++ {
			//fmt.Println("tick")
			//fmt.Println(stepValue + j)

			if j+stepValue == len(*recs) {
				fmt.Println("Length of recs")
				fmt.Println(len(*recs))

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

			pr := dynamodb.PutRequest{}
			pr.SetItem(av)
			wr := dynamodb.WriteRequest{}
			wr.SetPutRequest(&pr)

			wrArr = append(wrArr, &wr)

		}
		wrMap := make(map[string][]*dynamodb.WriteRequest, 1)

		wrMap[name] = wrArr

		arrayBatchRequest[i].SetRequestItems(wrMap)

	}
	return &arrayBatchRequest
}

// purgeTable calls delete on table entered.
// no take backs!
func purgeTable(sess *session.Session, tableToRemove string) {
	svc := dynamodb.New(sess)
	deleteTableInput := dynamodb.DeleteTableInput{}
	deleteTableInput.SetTableName(tableToRemove)
	result, err := svc.DeleteTable(&deleteTableInput)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(2)
	}

	fmt.Println("Removing Table " + tableToRemove)
	fmt.Println(result)
}

func buildCreateTableInput(tableName string, tableAtts []*dynamodb.AttributeDefinition, keySchema []*dynamodb.KeySchemaElement) dynamodb.CreateTableInput {

	ppr := "PAY_PER_REQUEST"
	cti := dynamodb.CreateTableInput{}

	fmt.Println("Table atts length" + string(len(tableAtts)))
	cti.AttributeDefinitions = tableAtts
	cti.KeySchema = keySchema
	cti.BillingMode = &ppr

	cti.TableName = &tableName

	return cti

}

func createDynamoDBTable(sess *session.Session, cti dynamodb.CreateTableInput) {

	svc := dynamodb.New(sess)
	result, err := svc.CreateTable(&cti)

	if err != nil {
		fmt.Println("Something went wrong")
		fmt.Println(err.Error())
		os.Exit(1)
	}
	fmt.Println(result)
	fmt.Println("Table Created")

}

func establishAWSSession() *session.Session {

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		//SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{Region: aws.String("ap-southeast-2")},
	}))

	return sess
}

func buildTableAttributes(rec record.Record) []*dynamodb.AttributeDefinition {

	arr := make([]*dynamodb.AttributeDefinition, 2)

	//var n string = "N"
	var s string = "S"
	tid := "TableID"
	rid := "RegionID"

	ad1 := dynamodb.AttributeDefinition{}

	ad1.AttributeName = &tid
	ad1.AttributeType = &s

	ad2 := dynamodb.AttributeDefinition{}
	ad2.AttributeName = &rid
	ad2.AttributeType = &s

	arr[0] = &ad1
	arr[1] = &ad2

	return arr

}

func buildKeySchema() []*dynamodb.KeySchemaElement {

	arr := make([]*dynamodb.KeySchemaElement, 2)
	hash := "HASH"
	rng := "RANGE"
	tid := "TableID"
	rid := "RegionID"

	kse1 := dynamodb.KeySchemaElement{}
	kse1.AttributeName = &rid
	kse1.KeyType = &hash

	kse2 := dynamodb.KeySchemaElement{}
	kse2.AttributeName = &tid
	kse2.KeyType = &rng

	arr[0] = &kse1
	arr[1] = &kse2

	return arr

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
	///home/gabe/go/src/csv_etl/out/2016Census_G02_AUS_SA4.csv.json
	args.InputFile = flag.String("f", "", "File to read, expected json format")
	args.Purge = flag.Bool("p", false, "Purge the DynamoDB Table")
	args.ConfirmPurge = flag.Bool("cp", false, "Second Required Flag to Confirm Purge of the DynamoDB Table")
	args.BuildTable = flag.Bool("b", false, "Build the table named by -n ")
	args.LoadData = flag.Bool("l", false, "load data specified in json file at location -f .")
	return args
}
