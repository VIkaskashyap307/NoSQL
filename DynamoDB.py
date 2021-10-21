# Author: Vikas Kashyap (vhuvinah)

import csv
import boto3

dynamoDB = boto3.resource('dynamodb',
                        region_name='us-west-1',
                        aws_access_key_id='XXXX',
                        aws_secret_access_key='YYYY'
                        )

try:
        table = dynamoDB.create_table(
                TableName='hw3-dynamodb-table',
                KeySchema=[
                    {
                        'AttributeName': 'Id',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'Temp',
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'Temp',
                        'AttributeType': 'S'
                    },
                ],
                ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                )

except:
    table = dynamoDB.Table("hw3-dynamodb-table")

table.meta.client.get_waiter('table_exists').wait(TableName='hw3-dynamodb-table')

with open("/Users/vikaskashyap/Desktop/14848/Mini_Project/experiments.csv") as schema:
    csv_file = csv.reader(schema, delimiter=',', quotechar='|')
    for item in csv_file:

        if item[0].lower() == "Id":
            continue

        url = "https://hw3-nosql-aws.s3.us-west-1.amazonaws.com/" + item[4]

        insert_schema = {'Id': item[0], 'Temp': item[1],
                         'Conductivity': item[2], 'Concentration': item[3], 'URL': url}

        try:
            table.put_item(Item=insert_schema)
        except:
            print("Item is already present!")

table_value = table.get_item(
                Key={
                    'Id': '3',
                    'Temp': '-2.93'
                    }
                )
query = {
          'Id': '3',
           'Temp': '-2.93'
        }

query_value = table_value['Item']

print("Query")
print(query)

print("Response from AWS ")
print(table_value)