import boto3

def db_record_func(db):
    LocationUri = db.get('LocationUri',None)
    Name = db.get('Name',None)
    Description = db.get('Description')
    CatalogId = db.get('CatalogId')
    CreateTime = db.get('CreateTime')
    return Name, CatalogId, CreateTime, LocationUri, Description

def tbl_record_func(database_name,tbl) -> tuple():
      #print('tbl', tbl)
      name = tbl.get('Name',None)
      LocationUri = tbl.get('LocationUri',None)
      Description = tbl.get('Description')
      CreateTime = tbl.get('CreateTime')
      r = (database_name, name, CreateTime, LocationUri, Decription)
      #print (r)
      return r

def getGlueTables(db, func, region:str):
    database_name = db.get('Name')
    if not database_name or database_name == '':
      return
    client = boto3.client('glue', region_name=region)
    response = client.get_tables(DatabaseName = database_name)

    tables = []
    while 'NextToken' in response:
        tables += response['TableList']
        response = glue_client.get_tables(DatabaseName = database_name, NextToken=response['NextToken'])
    tables += response['TableList']
    
    for tbl in tables:
        print(tbl['Name'])
        return func(database_name, tbl)
      
def getGlueDatabases(func1, func2, region:str):
    client = boto3.client('glue', region_name=region)
    response = client.get_databases()

    databases = []
    while 'NextToken' in response:
        databases += response['DatabaseList']
        response = glue_client.get_databases(NextToken=response['NextToken'])
    databases += response['DatabaseList']
    for db in databases:
        print(db['Name'])
        yield func1(db, func2, region)