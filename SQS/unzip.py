import boto3
import re
import sys

sqs = boto3.client('sqs')
url = sqs.get_queue_url(QueueName='sample_site_queue')

while sqs.get_queue_attributes(QueueUrl=url['QueueUrl'], AttributeNames=['ApproximateNumberOfMessages']) > 1:
	response = sqs.receive_message(
    QueueUrl=url['QueueUrl'],
    AttributeNames=['SentTimestamp'],
    MaxNumberOfMessages=1,
    MessageAttributeNames=['Site'],
    VisibilityTimeout=0,
    WaitTimeSeconds=0
	)

	if 'Messages' in response:
		message = response['Messages'][0]
		receipt_handle = message['ReceiptHandle']

		print message

		sqs.delete_message(QueueUrl=url['QueueUrl'], ReceiptHandle=receipt_handle)
	else:
		print response





