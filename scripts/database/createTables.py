import boto3

dynamodb = boto3.resource('dynamodb',region_name='us-west-2',endpoint_url='http://localhost:8000')

table = dynamodb.create_table(
	TableName='episodes',
	KeySchema=[
		{
			'AttributeName': 'seriesTconst',
			'KeyType': 'HASH'  #Partition key
		},
		{
			'AttributeName': 'episodeTconst',
			'KeyType': 'RANGE' #Sort key
		}
	],
	AttributeDefinitions=[
		{
			'AttributeName': 'seriesTconst',
			'AttributeType': 'S'
		},
		{
			'AttributeName': 'episodeTconst',
			'AttributeType': 'S'
		}
	],
	ProvisionedThroughput={
		'ReadCapacityUnits': 10,
		'WriteCapacityUnits': 10
	}
)