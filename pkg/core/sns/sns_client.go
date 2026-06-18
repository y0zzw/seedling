package sns

import (
	"context"
	"log"
	"sync"

	cfg "v-gnms/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
)

var snsClient *sns.Client
var once sync.Once

func GetSnsClient() *sns.Client {
	once.Do(func() {

		// Load the Shared AWS Configuration (~/.aws/config)
		aws_cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(
				cfg.GetConfigValue("AWS_REGION"),
			),
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					cfg.GetConfigValue("AWS_IAM_ACCESS_KEY_ID"),
					cfg.GetConfigValue("AWS_IAM_SECRET_ACCESS_KEY"),
					"",
				),
			),
		)
		if err != nil {
			log.Fatal(err)
		}

		// Create an Amazon S3 service client
		snsClient = sns.NewFromConfig(aws_cfg)
	})

	return snsClient
}

// Publish publishes a message to an Amazon SNS topic. The message is then sent to all
// subscribers. When the topic is a FIFO topic, the message must also contain a group ID
// and, when ID-based deduplication is used, a deduplication ID. An optional key-value
// filter attribute can be specified so that the message can be filtered according to
// a filter policy.
func PublishSnsMessage(message string, subject string, groupId string, dedupId string, filterKey string, filterValue string) error {
	topicArn := cfg.GetConfigValue("AWS_SNS_TOPIC_ARN")

	publishInput := sns.PublishInput{TopicArn: aws.String(topicArn), Message: aws.String(message)}
	if subject != "" {
		publishInput.Subject = aws.String(subject)
	}
	if groupId != "" {
		publishInput.MessageGroupId = aws.String(groupId)
	}
	if dedupId != "" {
		publishInput.MessageDeduplicationId = aws.String(dedupId)
	}
	if filterKey != "" && filterValue != "" {
		publishInput.MessageAttributes = map[string]types.MessageAttributeValue{
			filterKey: {DataType: aws.String("String"), StringValue: aws.String(filterValue)},
		}
	}
	_, err := snsClient.Publish(context.Background(), &publishInput)
	if err != nil {
		log.Printf("Couldn't publish message to topic %v. Here's why: %v", topicArn, err)
	}
	return err
}
