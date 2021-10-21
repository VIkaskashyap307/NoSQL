# Author: Vikas Kashyap (vhuvinah)

import boto3

s3 = boto3.resource('s3',
                    aws_access_key_id='XXXX',
                    aws_secret_access_key='YYYY'
                    )

s3.create_bucket(Bucket='hw3-nosql-aws', CreateBucketConfiguration={'LocationConstraint': 'us-west-1'})

bucket = s3.Bucket("hw3-nosql-aws")
bucket.Acl().put(ACL='public-read')

file1 = open('exp1.csv', 'rb')
file2 = open('exp2.csv', 'rb')
file3 = open('exp3.csv', 'rb')
schema = open('experiments.csv', 'rb')

s3.Object('hw3-nosql-aws', 'exp1.csv').put(Body=file1 )
s3.Object('hw3-nosql-aws', 'exp2.csv').put(Body=file2 )
s3.Object('hw3-nosql-aws', 'exp3.csv').put(Body=file3 )
s3.Object('hw3-nosql-aws', 'experiments.csv').put(Body=schema)

