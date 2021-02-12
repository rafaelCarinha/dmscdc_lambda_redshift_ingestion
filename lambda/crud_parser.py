class CRUDParser():

#Insert Operations
    def handleInsert(data, metadata):
        print('Handling SQL inserts')
        
        sql = '' + metadata['operation'] +  ' into ' + metadata['table-name'] + ' (' 
        for i in data: 
           obj = str(i)
           sql = sql + obj
           sql = sql +','
        sql = sql[:-1]    
        sql = sql + ') '
        sql = sql + ' values ('
        for i in data: 
           obj = str(i)
           val = str(data[obj]) 
           sql = sql + '\''+val+'\''
           sql = sql +','
        sql = sql[:-1]    
        sql = sql + ') '       
        print(sql)
        
        return sql
        
#Update Operations
    def handleUpdate(data, metadata):
        print('Handling SQL updates')
        
        sql = '' + metadata['operation'] +  ' ' + metadata['table-name'] + ' set ' 
        for i in data: 
           obj = str(i)
           val = str(data[obj])
           sql = sql + obj
           sql = sql + ' = '
           sql = sql + '\''+val+'\''
           sql = sql +','
        sql = sql[:-1]    
        sql = sql + ' where ID' 
        sql = sql + ' = '
        sql = sql + str(data['ID'])
        print(sql)
        
        return sql
        
#Delete Operations
    def handleDelete(data, metadata):
        print('Handling SQL deletes')
        
        sql = '' + metadata['operation'] +  ' from ' + metadata['table-name'] + ' ' 
        sql = sql + ' where ID' 
        sql = sql + ' = '
        sql = sql + str(data['ID'])     
        print(sql)
        
        return sql