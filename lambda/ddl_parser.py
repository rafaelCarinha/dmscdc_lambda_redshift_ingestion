class DDLParser():

    # Create Table Scripts
    def handleCreate(columns, metadata):
        print('Handling Create Table ')

        try:
            sql = 'CREATE TABLE ' + metadata['table-name'] + ' ('
        
            # Iterate Create table Columns
            for a in columns:
                print(a)
                sql = sql + ' ' + a + ''
        
                print(columns[a])
                for b in columns[a]:
                    obj = str(b)
                    val = columns[a][obj]
                    if obj == 'type':
                        if val == 'STRING':
                            length = columns[a]['length']
                            sql = sql + ' ' + 'varchar'
                            sql = sql + '('
                            sql = sql + str(length)
                            sql = sql + ')'
                        elif val == 'NUMERIC':
                            precision = columns[a]['precision']
                            scale = 0
                            try:
                                scale = columns[a]['scale']
                            except Exception as e:
                                print('Scale not present for Numeric column!')
                            sql = sql + ' ' + 'NUMERIC'
                            sql = sql + '('
                            sql = sql + str(precision)
                            if scale != 0:
                                sql = sql + ',' + str(scale)
                            sql = sql + ')'
                        elif val == 'CLOB':
                            length = 65535
                            sql = sql + ' ' + 'varchar'
                            sql = sql + '('
                            sql = sql + str(length)
                            sql = sql + ')'
                        else:
                            sql = sql + ' ' + str(val)
                    elif obj == 'nullable':
                        if not val:
                            sql = sql + ' NOT NULL'
                    print(b)
        
                sql = sql + ','
        
            sql = sql[:-1]
            sql = sql + ' )'
        
        except Exception as e:
            print('An error has occured: ' + e.__class__)
        
        print('Concatenated SQL:')
        print(sql)

        return sql
        
        
    # Drop Table Scripts
    def handleDrop(columns, metadata):
        print('Handling Drop Table ')

        try:
            sql = 'DROP TABLE ' + metadata['table-name']
        
        except Exception as e:
            print('An error has occured: ' + e.__class__)
        
        print('Concatenated SQL:')
        print(sql)

        return sql
        
    # Handling Add Columns
    def handleAddColumn(columnNames, oldTableDef, tableDef, metadata):
        print('Handling Add Columns ')

        sql = ''
        print('Column names:')

        for c in columnNames:
            sql = sql + ' ALTER TABLE ' + metadata['table-name'] + ' ADD COLUMN'
            col = tableDef['columns'][c]
            print(col)

            sql = sql + ' ' + c + ''
            for b in col:
                obj = str(b)
                val = tableDef['columns'][c][obj]
                if obj == 'type':
                    if val == 'STRING':
                        length = tableDef['columns'][c]['length']
                        sql = sql + ' ' + 'varchar'
                        sql = sql + '('
                        sql = sql + str(length)
                        sql = sql + ')'
                    elif val == 'NUMERIC':
                        precision = tableDef['columns'][c]['precision']
                        scale = 0
                        try:
                            scale = tableDef['columns'][c]['scale']
                        except Exception as e:
                            print('Scale not present for Numeric column!')
                        sql = sql + ' ' + 'NUMERIC'
                        sql = sql + '('
                        sql = sql + str(precision)
                        if scale != 0:
                            sql = sql + ',' + str(scale)
                        sql = sql + ')'
                    else:
                        sql = sql + ' ' + str(val)
                elif obj == 'nullable':
                    if not val:
                        sql = sql + ' NOT NULL'
                print(b)

            sql = sql + ';'

        sql = sql[:-1]

        print('Concatenated SQL:')
        print(sql)

        return sql

    # Handling Drop Column
    def handleDropColumn(columnNames, oldTableDef, tableDef, metadata):
        print('Handling Drop Columns ')

        sql = ''

        print('Column names:')

        for c in columnNames:
            sql = sql + ' ALTER TABLE ' + metadata['table-name'] + ' DROP COLUMN'
            col = c
            print(col)

            sql = sql + ' ' + c + ''

            sql = sql + ';'

        sql = sql[:-1]

        print('Concatenated SQL:')
        print(sql)

        return sql


    # Handling Alter Column
    def handleColumnTypeChange(columnNames, oldTableDef, tableDef, metadata):
        print('Handling Alter Column ')

        sql = ''

        print('Column names:')

        for c in columnNames:
            sql = sql + ' ALTER TABLE ' + metadata['table-name'] + ' ALTER COLUMN'
            col = tableDef['columns'][c]
            print(col)

            sql = sql + ' ' + c + ''
            for b in col:
                obj = str(b)
                val = tableDef['columns'][c][obj]
                if obj == 'type':
                    if val == 'STRING':
                        length = tableDef['columns'][c]['length']
                        sql = sql + ' type ' + 'varchar'
                        sql = sql + '('
                        sql = sql + str(length)
                        sql = sql + ')'
                    elif val == 'NUMERIC':
                        precision = tableDef['columns'][c]['precision']
                        scale = 0
                        try:
                            scale = tableDef['columns'][c]['scale']
                        except Exception as e:
                            print('Scale not present for Numeric column!')
                        sql = sql + ' ' + 'NUMERIC'
                        sql = sql + '('
                        sql = sql + str(precision)
                        if scale != 0:
                            sql = sql + ',' + str(scale)
                        sql = sql + ')'
                    else:
                        sql = sql + ' ' + str(val)
                elif obj == 'nullable':
                    if not val:
                        sql = sql + ' NOT NULL'
                print(b)

            sql = sql + ';'

        sql = sql[:-1]

        print('Concatenated SQL:')
        print(sql)

        return sql
