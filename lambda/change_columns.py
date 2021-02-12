from ddl_parser import DDLParser
from util import Util

class ChangeColumns():

    # Handling Change Columns(Add/Drop columns)
    def handleChangeColumns(columnNames, oldTableDef, tableDef, metadata):
        print('Handling Change Columns(Add/Drop columns)')

        sql = ''
        print('Column names length: ' + str(len(columnNames)))
        print('Old table def size: ' + str(Util.returnListSize(oldTableDef['columns'])))
        print('New table def size: ' + str(Util.returnListSize(tableDef['columns'])))
        oldTableDefSize = str(Util.returnListSize(oldTableDef['columns']))
        newTableDefSize = str(Util.returnListSize(tableDef['columns']))

        if oldTableDefSize < newTableDefSize:
            sql = sql + DDLParser.handleAddColumn(columnNames, oldTableDef, tableDef, metadata)
        else:
            sql = sql + DDLParser.handleDropColumn(columnNames, oldTableDef, tableDef, metadata)

        print('Concatenated SQL:')
        print(sql)

        return sql