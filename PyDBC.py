# Driver Install Command
# $ pip3 install --upgrade PyMySQL

import pymysql
import sys

"""
** Python Database Connectivity

 * @author Ma Xuefeng

 * @date 2018/7/14

 * @version v1.0.0
"""
class PyDBC:
    """
    Python Database Connectivity
    """

    # Configer
    host = 'localhost'
    user = 'user'
    password = 'password'
    db = 'database_name'

    # is_debug : True  - print(sql)
    #            False - silence
    is_debug = True
    # Other Parameter
    connection = None

    def __init__(self):
        """ Initialize """
        # Create database connection
        self.connection = pymysql.connect(host = self.host,
                                         user = self.user,
                                         password = self.password,
                                         db = self.db)

    def close(self):
        """ Close """
        self.connection.close()

    def execute(self, sql, is_exe=False):
        """
        Fetch Rows from Table

        :param sql: sql statements
        :param is_exe: if is execute : True
                       if is query   : False
        :return: if execute : row count affected
                  if query  :  the result
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            if is_exe:
                result = cursor.rowcount
                self.connection.commit()
            else:
                result = cursor.fetchall()
        return result

    def get_all(self, table, columns=None, conditions=None):
        """
        Fetch Rows from Table

        :param table: table name ( type : string)
        :param column: column name ( type : list)
        :param conditions:  key & value ( type : dictionary)
        :return: rows
        """
        sql = 'select '
        if columns != None and len(columns)>0:
            for index, column in enumerate(columns):
                if index > 0:
                    sql += ' , '
                sql += column
        else:
            sql += ' * '
        sql += ' from ' + table
        if conditions != None and len(conditions) > 0:
            sql += ' where '
            for index, (key, value) in enumerate(conditions.items()):
                if index > 0:
                    sql += ' && '
                sql += key+" = '"+conditions[key]+"'"
        if self.is_debug:
            print('SQL in [DB.get_all] : ' + sql)
        results = self.execute(sql, False)
        if len(results) < 1:
            results = None
        return results

    def get_one(self, table, columns=None, conditions=None):
        """
        Fetch One Row from Table

        :param table: table name ( type : string)
        :param column: column name ( type : list)
        :param conditions:  key & value ( type : dictionary)
        :return: rows
        """
        result = self.get_all(table, columns, conditions)
        if len(result) > 0 :
            result = result[0]
        else:
            result = None
        return result

    def save(self, table, rows):
        """
        Insert Data into Table

        :param table: table name ( type : string)
        :param rows: key & values ( type : dictionary )
        :return: row_count affected
        """
        key =' ( '
        value = '( '
        for index, (key_, value_) in enumerate(rows.items()):
            if index > 0:
                key += ' , '
                value += ' , '
            key += key_
            value += "'" + value_ + "'"
        key += ' ) '
        value += ' ) '
        sql = 'insert into ' + table + key + ' value ' + value
        if self.is_debug:
            print('SQL in [DB.save] : ' + sql)
        row_count = self.execute(sql, True)
        if row_count < 1:
            sys.stderr.write('Fail in saving : SQL in [DB.save] ==> ' + sql + '\n')
        return row_count

    def update(self, table, columns, conditions):
        """
        Update Data

        :param table: table name ( type : string )
        :param columns: key & values ( type : dictionary )
        :param conditions: key & values ( type : dictionary )
        :return: row count affected
        """
        set_sql = ''
        conditions_sql = ''
        for index,( key, value) in enumerate(columns.items()):
            if index > 0 :
                set_sql += ' , '
            set_sql += key + " = '" + value + "' "
        for index, (key, value) in enumerate(conditions.items()):
            if index > 0:
                conditions_sql += ' && '
            conditions_sql += key + " = '" + value + "' "
        sql = 'update ' + table + ' set ' + set_sql + ' where ' + conditions_sql
        if self.is_debug:
            print(sql)
        row_count = self.execute(sql, True)
        if row_count < 1:
            sys.stderr.write('Fail in updating : SQL in [DB.save] ==> ' + sql + '\n')
        return row_count

    def delete(self, table, conditions):
        """
        Delete Data

        :param table: table name ( type : string )
        :param conditions: key & values ( type : dictionary )
        :return: row count affected
        """
        sql = 'delete from ' + table + ' where '
        for index,( key, value) in enumerate(conditions.items()):
            if index > 0:
                sql += ' && '
            sql += key + " = '" + value + "'"
        if self.is_debug:
            print(sql)
        row_count = self.execute(sql, True)
        if row_count < 1:
            sys.stderr.write('Fail in updating : SQL in [DB.save] ==> ' + sql + '\n')
        return row_count