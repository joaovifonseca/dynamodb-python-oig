import json
import boto3
import uuid
from boto3.dynamodb.conditions import Key

dynamodb = boto3.client('dynamodb')

dynamodbs = boto3.resource('dynamodb')

table_1 = 'oig_get'
table_2 = 'oig_post'

def generate_id():
    return uuid.uuid4()

def insert_all_tables(document, type_person):
    table = dynamodbs.Table(table_2)
    
    response = table.query(KeyConditionExpression=Key('pessoa').eq(document + '#' + type_person))
    
    if response['Count'] > 0:
        return response['Items']
    else:
        id_client = str(generate_id())
        dynamodb.put_item(TableName=table_1, Item={'id_cliente':{'S':id_client},'pessoa':{'S':document + '#' + type_person}})
        dynamodb.put_item(TableName=table_2, Item={'pessoa':{'S':document + '#' + type_person}, 'id_cliente':{'S':id_client}})
    

def lambda_handler(event, context):
    tipo_pessoa = 'F'
    documento = '47270137896'
    
    response = insert_all_tables(documento, tipo_pessoa)
    
    
    return {
        'statusCode': 200,
        'body': response
    }
