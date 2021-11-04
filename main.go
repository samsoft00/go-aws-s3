package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Function to upload item
func uploadItem(sess *session.Session) {
	f, err := os.Open("03-downloads-and-uploads/my-file.ext")
	if err != nil {
		log.Fatal("could not open file")
	}

	defer f.Close()

	uploader := s3manager.NewUploader(sess)
	result, err := uploader.Upload(&s3manager.UploadInput{
		ACL: aws.String("public-read"),
		Bucket: aws.String("go-aws-s3"),
		Key: aws.String("my-file.txt"),
		Body: f,
	})

	if err != nil {
		log.Fatal(err.Error())
	}

	log.Printf("Upload Result: %+v\n", result)
}

// Listing items in a bucket
func listBucketItems(sess *session.Session){
	svc := s3.New(sess)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String("go-aws-s3"),
	})

	if err != nil {}
	for _, item := range resp.Contents {
		fmt.Println("Name:		", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:		", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("")
	}
}

// Download items
func downloadItem(sess *session.Session) {
	file, err := os.Create("03-downloads-and-uploads/downloaded.txt")
	if err != nil {}	
	defer file.Close()

	downloader := s3manager.NewDownloader(sess)

	// number of bytes downloaded or error
	if _, err = downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String("go-aws-s3-course"),
		Key: aws.String("my-file.txt"),
	}); err != nil {
		log.Fatal(err.Error())
	}

	log.Println("Successfully downloaded!")
}

func deleteItem(sess *session.Session) {
	svc := s3.New(sess)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String("go-aws-s3-course"),
		Key: aws.String("my-file.txt"),
	}

	result, err := svc.DeleteObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code(){
			default:
				log.Fatal(aerr.Error())
			}
		} else {
			log.Fatal(err.Error())
		}
	}

	log.Printf("Result: %+v\n", result)
}

func main() {
	fmt.Println("Listing Buckets")

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2")},
	)

	if err != nil {
		log.Fatal(err.Error())
	}

	svc := s3.New(sess)

	//1. List all bucket in s3
	result, err := svc.ListBuckets(nil)
	if err != nil {
		log.Fatalf("Unable to list buckets, %v", err)
	}

	for _, b := range result.Buckets {
		fmt.Printf("* %s creaated on %s\n", aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}

	//2. Create Bucket
	input := &s3.CreateBucketInput{
		Bucket: aws.String("go-aws-s3"),
	}

	resp, err := svc.CreateBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				fmt.Println(s3.ErrCodeBucketAlreadyExists, aerr.Error())
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				fmt.Println(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(aerr.Error())
		}
		return
	}

	log.Print(resp)

	//3. Upload file
	uploadItem(sess)

	//4. List bucket items
	listBucketItems(sess)

	// 5. Download files
	downloadItem(sess)

	//6. Delete file from s3 bucket
	deleteItem(sess)
}