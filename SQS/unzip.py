import boto3
import re
import os
import sys

sqs = boto3.client('sqs')
s3 = boto3.client('s3')
url = sqs.get_queue_url(QueueName='sample_site_queue')
bucket = 'hausmanbucket'

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

		for file in os.listdir(message['MessageAttributes']['Site']['StringValue']):
			path = message['MessageAttributes']['Site']['StringValue'] + '/'
			name = os.path.basename(file)
			key = message['MessageAttributes']['Site']['StringValue'] + '_' + file
			with open(path + file, 'rb') as data:
    				s3.upload_fileobj(data, bucket, key)

		os.system('rm -rf ' + message['MessageAttributes']['Site']['StringValue'])
		os.system('rm ' + message['Body'])

		sqs.delete_message(QueueUrl=url['QueueUrl'], ReceiptHandle=receipt_handle)
	else:
		print response





