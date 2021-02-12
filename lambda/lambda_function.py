import json
import base64
import logging
import boto3
from boto3 import client as boto3_client
from crud_parser import CRUDParser
from ddl_parser import DDLParser
from change_columns import ChangeColumns

def lambda_handler(event, context):
    
    print(event)
    
    dbChange=''
    sql = ''
    httpStatus = 200
    
    try:
        for record in event['Records']:
    
            #Kinesis data is base64 encoded so decode here
            payload=base64.b64decode(record["kinesis"]["data"])
            print(record)
            dbChange=payload
            y=json.loads(dbChange)
            formattedJson=json.dumps(y, indent=4, sort_keys=True)
            print(formattedJson)
            print("Decoded payload: " + str(y))
            #data = y['data']
            metadata = y['metadata']
    
            #Decide the type of operation
            if(metadata['operation'] == 'insert'):
                sql = CRUDParser.handleInsert(y['data'], metadata)
            elif(metadata['operation'] == 'update'):
                sql = CRUDParser.handleUpdate(y['data'], metadata)
            elif(metadata['operation'] == 'delete'):
                sql = CRUDParser.handleDelete(y['data'], metadata)
            elif(metadata['operation'] == 'create-table'):
                columns = y['control']['table-def']['columns']
                #pK = y['control']['table-def']['primary-key']
                sql = DDLParser.handleCreate(columns, metadata)
            elif(metadata['operation'] == 'drop-table'):
                columns = y['control']['table-def']['columns']
                sql = DDLParser.handleDrop(columns, metadata)
            elif (metadata['operation'] == 'add-column'):
                columnNames = y['control']['column-names']
                oldTableDef = y['control']['old-table-def']
                tableDef = y['control']['table-def']
                sql = DDLParser.handleAddColumn(columnNames, oldTableDef, tableDef, metadata)
            elif (metadata['operation'] == 'drop-column'):
                columnNames = y['control']['column-names']
                oldTableDef = y['control']['old-table-def']
                tableDef = y['control']['table-def']
                sql = DDLParser.handleDropColumn(columnNames, oldTableDef, tableDef, metadata)
            elif (metadata['operation'] == 'column-type-change'):
                columnNames = y['control']['column-names']
                oldTableDef = y['control']['old-table-def']
                tableDef = y['control']['table-def']
                sql = DDLParser.handleColumnTypeChange(columnNames, oldTableDef, tableDef, metadata)
            elif (metadata['operation'] == 'change-columns'):
                columnNames = y['control']['column-names']
                oldTableDef = y['control']['old-table-def']
                tableDef = y['control']['table-def']
                sql = ChangeColumns.handleChangeColumns(columnNames, oldTableDef, tableDef, metadata)
            else:
                print('Unsupported operation')
                httpStatus = 400
    
            #Send SQL To Redshift
            lambda_client = boto3_client('lambda', region_name="us-west-2")
            lambda_client.invoke(
                FunctionName="ellis-ongoing-dms-redshift",
                InvocationType='Event',
                Payload=json.dumps(sql))
                
    except Exception as e:
        print('An error has occured: ' + e.__class__)
    
    return {
        'statusCode': httpStatus,
        'body': json.dumps(sql)
    }
