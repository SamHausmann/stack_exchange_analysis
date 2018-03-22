import boto3
import re

sqs = boto3.client('sqs')

queue = sqs.create_queue(QueueName='sample_site_queue')

f = inputfile = open('sites.txt', 'r')
siteList = list(f)

specialCases = ['askubuntu.com.7z', 'superuser.com.7z']

for site in siteList:

	sitePattern = '.*?.(com|net|com-.*).7z'
	siteRes = re.search(sitePattern, site)

	if siteRes:
		site = siteRes.group()
	else:
		for case in specialCases:
			specialCaseRes = re.search(case, site)
			if specialCaseRes:
				site = specialCaseRes.group()

	namePattern = '([^.]*)'
	nameRes = re.search(namePattern, site)
	if nameRes:
		siteName = nameRes.group(1)
		print siteName

		response = sqs.send_message(
		    QueueUrl=queue['QueueUrl'],
		    DelaySeconds=10,
		    MessageAttributes={
		        'Site': {
		            'DataType': 'String',
		            'StringValue': siteName
		        }
		    },
		    MessageBody=(
		        site
		    )
		)

		print(response['MessageId'])

	