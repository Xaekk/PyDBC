# Driver Install Command
# $ pip3 install --upgrade PyMySQL

import pymysql
import sys
from tqdm import tqdm
import threading

"""
** Python Database Connectivity

 * @author Ma Xuefeng

 * @date 2018/11/19

 * @version v2.0.0
"""
class PyDBC:
    """
    Python Database Connectivity
    """

    # Configer
    host = 'localhost'
    user = 'user_name'
    password = 'password'
    db = 'database_name'

    # is_debug : True  - print(sql)
    #            False - silence
    is_debug = False
    save_many_batch = 10000 # 0 means unlimit
    thread_amount = 10
    # Other Parameter
    connection = None
    lock = threading.Lock() 

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
        self.lock.acquire()
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            if is_exe:
                result = cursor.rowcount
                self.connection.commit()
            else:
                result = cursor.fetchall()
        self.lock.release()
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
                sql += "{} = '{}'".format(key, conditions[key])
        if self.is_debug:
            print('SQL in [PyDBC.get_all] : ' + sql)
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
        if result != None and len(result) > 0 :
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
            if len('{}'.format(value_))>0:
                value += "'{}'".format(value_)
            else:
                value += "''"
        key += ' ) '
        value += ' ) '
        sql = 'insert into ' + table + key + ' value ' + value
        if self.is_debug:
            print('SQL in [PyDBC.save] : ' + sql)
        row_count = self.execute(sql, True)
        if row_count < 1:
            sys.stderr.write('Fail in saving : SQL in [DB.save] ==> ' + sql + '\n')
        return row_count


    def save_many_worker(self, table, row, values_batch):
        self.save_many(table, row, values_batch)

    def save_many_by_batch(self, table, row, values):
        """
        Insert Many Data into Table
        :warning row & value must be the same order
        :param table: table name ( type : string)
        :param rows: row name ( type : list )
        :param values: values ( type : list of list )
        :return: row_count affected
        """
        values_batches = []
        if self.save_many_batch > 0:
            loops = len(values) // self.save_many_batch
            print('Preparing loops...')
            for index in tqdm(range(loops+1)):
                if index < loops:
                    values_batches.append(values[index * self.save_many_batch: (index+1) * self.save_many_batch])
                else:
                    values_batches.append(values[index * self.save_many_batch:])
        else:
            values_batches.append(values[:])

        print('Saving...')
        threads = []
        for values_batch in tqdm(values_batches):
            t = threading.Thread(target=self.save_many_worker, args=(table, row, values_batch,))
            t.start()
            threads.append(t)
            if len(threads) > (self.thread_amount - 1 if self.thread_amount > 0 else 0):
                for t in threads:
                    t.join()
                threads = []
        for t in threads:
            t.join()


    def save_many(self, table, row, values):
        """
        Insert Many Data into Table
        :warning row & value must be the same order
        :param table: table name ( type : string)
        :param rows: row name ( type : list )
        :param values: values ( type : list )
        :return: row_count affected
        """
        if self.is_debug:
            print('Preparing Keys...')
        key =' ( '
        def add_keys(index, key, key_):
            if index > 0:
                key += ' , '
            key += key_
            return key
        for index, key_ in enumerate(row):
            key = add_keys(index, key, key_)
        key += ' ) '

        if self.is_debug:
            print('Prepare Values...')
        value = ''
        def add_values(index, value, value_row):
            if index > 0:
                value += ','
            value += '('
            for i, value_ in enumerate(value_row):
                if i > 0:
                    value += ','
                value += "'{}'".format(value_) if value_ != None else "{}".format('NULL')
            value += ')'
            return value
        for index, value_row in enumerate(values):
            value = add_values(index, value, value_row)

        sql = 'insert into {} {} values {} '.format(table, key, value)
        if self.is_debug:
            print('SQL in [PyDBC.save_many] : ' + sql)
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
            set_sql += " {} = '{}'".format(key, value)
        for index, (key, value) in enumerate(conditions.items()):
            if index > 0:
                conditions_sql += ' && '
            conditions_sql += " {} = '{}'".format(key, value)
        sql = 'update ' + table + ' set ' + set_sql + ' where ' + conditions_sql
        if self.is_debug:
            print(sql)
        row_count = self.execute(sql, True)
        if row_count < 1:
            sys.stderr.write('Fail in updating : SQL in [PyDBC.save] ==> ' + sql + '\n')
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
            sql += "{} = '{}'".format(key, value)
        if self.is_debug:
            print(sql)
        row_count = self.execute(sql, True)
        if row_count < 1:
            sys.stderr.write('Fail in updating : SQL in [PyDBC.save] ==> ' + sql + '\n')
        return row_count
        
    def query_close(self, table, columns=None, conditions=None):
        datalist = self.get_all(table=table, columns=columns, conditions=conditions)
        self.close()
        return datalist