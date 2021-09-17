import json
import boto3
import uuid
from boto3.dynamodb.conditions import Key
import random

dynamodb = boto3.client('dynamodb')

dynamodbs = boto3.resource('dynamodb')

table_1 = 'oig_get'
table_2 = 'oig_post'

def generate_id():
    return uuid.uuid4()
    
def generate_cpf():                                                        
    cpf = [random.randint(0, 9) for x in range(9)]                              
                                                                                
    for _ in range(2):                                                          
        val = sum([(len(cpf) + 1 - i) * v for i, v in enumerate(cpf)]) % 11      
                                                                                
        cpf.append(11 - val if val > 1 else 0)                                  
                                                                                
    return '%s%s%s%s%s%s%s%s%s%s%s' % tuple(cpf)
    
def generate_cnpj():                                                       
    def calculate_special_digit(l):                                             
        digit = 0                                                               
                                                                                
        for i, v in enumerate(l):                                               
            digit += v * (i % 8 + 2)                                            
                                                                                
        digit = 11 - digit % 11                                                 
                                                                                
        return digit if digit < 10 else 0                                       
                                                                                
    cnpj =  [1, 0, 0, 0] + [random.randint(0, 9) for x in range(8)]             
                                                                                
    for _ in range(2):                                                          
        cnpj = [calculate_special_digit(cnpj)] + cnpj                           
                                                                                
    return '%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % tuple(cnpj[::-1])

def insert_all_tables(document, type_person):
    table = dynamodbs.Table(table_2)
    
    response = table.query(KeyConditionExpression=Key('pessoa').eq(document + '#' + type_person))
    
    print(generate_cnpj())
    print(generate_cpf())
    
    if response['Count'] > 0:
        return response['Items']
    else:
        id_client = str(generate_id())
        dynamodb.put_item(TableName=table_1, Item={'id_cliente':{'S':id_client},'pessoa':{'S':type_person + '#' + document}})
        dynamodb.put_item(TableName=table_2, Item={'pessoa':{'S': type_person + '#' + document}, 'id_cliente':{'S':id_client}})
        

def insert_without_consult(document, type_person):
    table = dynamodbs.Table(table_2)
    id_client = str(generate_id())
    dynamodb.put_item(TableName=table_1, Item={'id_cliente':{'S':id_client},'pessoa':{'S':type_person + '#' + document}})
    dynamodb.put_item(TableName=table_2, Item={'pessoa':{'S': type_person + '#' + document}, 'id_cliente':{'S':id_client}})
    
def generate_insert_person(quantity):
    for x in range(quantity):
       cpf = generate_cpf()
       insert_without_consult(cpf, 'F')
       
def generate_insert_company(quantity):
    for x in range(quantity):
       cnpj = generate_cnpj()
       insert_without_consult(cnpj, 'J')
    

def lambda_handler(event, context):
    
    #Para inserir/ consultar de forma normal use as tres linhas de código abaixo
    '''
    tipo_pessoa = 'F'
    documento = '47270137896'
    response = insert_all_tables(documento, tipo_pessoa)
    '''
    
    #Para gerar massa pj use o metodo e passe a quantidade de vezes
    #generate_insert_company(3000)
    
    #Para gerar massa pf use o metodo e passe a quantidade de vezes
    #generate_insert_person(3000)
    
    return {
        'statusCode': 200,
        'body': 'lambada'
    }
