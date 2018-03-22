import boto3
import re
import pylzma
import os
import sys

sqs = boto3.client('sqs')
url = sqs.get_queue_url(QueueName='sample_site_queue')

i = 0
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

		os.system('wget https://ia800107.us.archive.org/27/items/stackexchange/' + message['Body'])
		os.system('7z x ' + message['Body'] + ' -o' + message['MessageAttributes']['Site']['StringValue'])

		# put dir into S3

		sys.exit()

		sqs.delete_message(QueueUrl=url['QueueUrl'], ReceiptHandle=receipt_handle)
	else:
		print response





