# Driver Install Command
# $ pip3 install --upgrade PyMySQL

import pymysql
import sys
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock
import math
from queue import Queue
from tqdm import tqdm
"""
** Python Database Connectivity
 * @author Ma Xuefeng
 * @date 2019/7/25
 * @version v3.0.0
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
    # Other Parameter
    connection = None


    def __init__(self, host=None, user=None, password=None, db=None):
        """ Initialize """
        # Create database connection
        self.host = host if host != None else self.host
        self.user = user if user != None else self.user
        self.password = password if password != None else self.password
        self.db = db if db != None else self.db

        self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, use_unicode=True, charset='utf8')


    def close(self):
        """ Close """
        self.connection.close()


    def execute_sql(self, sql):
        """
        Execute SQL
        :param sql: sql statements (type: string)
        :return: row_count affected
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.rowcount
            self.connection.commit()
        return result


    def execute(self, sql, datas=()):
        """
        Fetch Rows from Table
        :param sql: sql statements
        :param datas: execute data (type: list)
        :return: if execute : row count affected
                  if query  :  the result
        """
        with self.connection.cursor() as cursor:
            if len(datas) == 1:
                cursor.execute(sql, list(datas)[0])
                result = cursor.rowcount
                self.connection.commit()
            elif len(datas) > 1:
                cursor.executemany(sql, list(datas))
                result = cursor.rowcount
                self.connection.commit()
            else:
                cursor.execute(sql)
                result = cursor.fetchall()
        return result


    def get(self, table, columns=None, conditions=None, limit=None, more_command=None):
        """
        Fetch Rows from Table
        :param table: table name ( type : string)
        :param column: column name ( type : list)
        :param conditions:  key & value ( type : dictionary)
        :param limit: limit count of resualt (type: int)
        :param more_command: more command add at the last (type: string)
        :return: rows
        """
        def del_space(str_):
            while str_.__class__ == str and '  ' in str_:
                str_ = str_.replace('  ', ' ')
            return str_
        def judge_condition(k, v):
            v = del_space(v)
            if v in [del_space('{} {} {}'.format(i, n, nu))
                     for i in ['IS', 'Is', 'is']
                     for n in ['NOT', 'Not', 'not', '']
                     for nu in ['NULL', 'Null', 'null']]:
                return ' {} {}'.format(k, v)
            elif v.__class__ == int:
                return "{}={}".format(k, v)
            if v.__class__ == str:
                return "{} LIKE '{}'".format(k, v)

        columns_str = ' , '.join(columns) if columns != None else '*'
        conditions_str = ' WHERE ' + ' AND '.join([judge_condition(k, v) for k, v in conditions.items()]) if conditions != None else ''
        limit_str = ' LIMIT {}'.format(limit) if limit != None else ''
        more_command_str = more_command if more_command.__class__ == str else ''

        sql = 'select {0} from {1} {2} {3} {4}'.format(columns_str, table, conditions_str, limit_str, more_command_str)

        if self.is_debug:
            print('SQL in [PyDBC.get] : ' + sql)
        results = self.execute(sql)
        if len(results) < 1:
            results = []

        return results


    def save(self, table, rows):
        """
        Insert Data into Table
        :param table: table name ( type : string)
        :param rows: key & values ( type : dictionary )
        :return: row_count affected
        """
        key_str = ' ,'.join(rows.keys())
        values = rows.values()
        values_str = " ,".join(['%s']*len(values))

        sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(table, key_str, values_str)
        if self.is_debug:
            print('SQL in [PyDBC.save] : ' + sql)
            print(values)
        row_count = self.execute(sql, values)
        if row_count < 1:
            sys.stderr.write('Fail in saving : SQL in [PyDBC.save] ==> ' + sql + '\n')
            sys.stderr.write('Fail in saving : Datas in [PyDBC.save] ==> ' + str(list(values)) + '\n')
        return row_count


    def save_many(self, table, columns, rows):
        """
        Insert many data into Table
        :param table: table name (type : string)
        :param columns: columns names (type: list)
        :param rows: data rows (type: list of list))
        :return: row_count affected
        """
        key_str = ' ,'.join(columns)
        values = rows
        values_str = " ,".join(['%s'] * len(columns))

        sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(table, key_str, values_str)
        if self.is_debug:
            print('SQL in [PyDBC.save_many] : ' + sql)
            print(values)
        row_count = self.execute(sql, values)
        if row_count < 1:
            sys.stderr.write('Fail in saving : SQL in [PyDBC.save] ==> ' + sql + '\n')
            sys.stderr.write('Fail in saving : Datas in [PyDBC.save] ==> ' + str(list(values)) + '\n')
        return row_count


    def save_many_batch(self, table, columns, rows, batch_size, workers=None, pool_size=10):
        """
        Insert many data by Batch into Table
        :param table: table name (type : string)
        :param columns: columns names (type: list)
        :param rows: data rows (type: list of list))
        :param batch_size: batch size (type: int)
        :param workers: workers size (type: int)
        :param pool_size: pool size (type: int)
        :return: row_count affected
        """
        key_str = ' ,'.join(columns)
        values_str = " ,".join(['%s'] * len(columns))

        sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(table, key_str, values_str)

        batchs = [rows[n*batch_size: (n+1)*batch_size] for n in range(math.ceil(len(rows)/batch_size))]

        pyDBC_Pools = Queue()
        pyDBC_Pools.put(self)
        [pyDBC_Pools.put(PyDBC(host=self.host, user=self.user, password=self.password, db=self.db)) for _ in range(pool_size-1)]

        row_count = 0
        lock = Lock()
        def save_worker(sql, values):
            nonlocal row_count
            pyDBC = pyDBC_Pools.get()
            try:
                r_c = pyDBC.execute(sql, values)
                lock.acquire()
                row_count += r_c
                lock.release()
            except BaseException:
                pass
            finally:
                pyDBC_Pools.put(pyDBC)
        with ThreadPoolExecutor(max_workers=workers) if workers.__class__ == int else ThreadPoolExecutor() as executor:
            list(tqdm(executor.map(lambda batch: save_worker(sql, batch), batchs), total=len(batchs)))

        list(map(lambda p: p.close() if p != self else None, pyDBC_Pools.queue))
        return row_count


    def update(self, table, columns, conditions=None):
        """
        Update Data
        :param table: table name (type : string)
        :param columns: key & values (type : dict)
        :param conditions: key & values ( type : dict )
        :return: row_count affected
        """
        columns_str = ' , '.join(['{} = %s'.format(k) for k in columns.keys()])
        conditions_str = ' WHERE ' + ' AND '.join(["{} = %s".format(k) for k in conditions.keys()]) if conditions != None else ''

        sql = 'UPDATE {0} SET {1} {2}'.format(table, columns_str, conditions_str)
        if self.is_debug:
            print('SQL in [PyDBC.update] : ' + sql)
        row_count = self.execute(sql, [list(columns.values()) + list(conditions.values())])
        return row_count


    def delete(self, table, conditions=None):
        """
        Delete Data
        :param table: table name ( type : string )
        :param conditions: key & values ( type : dictionary )
        :return: row count affected
        """
        def judge_condition(k, v):
            if v.__class__ == int:
                return '{} = %s'.format(k, v)
            elif v.__class__ == str:
                return "{} LIKE %s".format(k, v)
        conditions_str = ' WHERE ' + ' AND '.join([judge_condition(k, v) for k, v in conditions.items()]) if conditions != None else ''

        sql = 'DELETE FROM {0} {1}'.format(table, conditions_str)
        if self.is_debug:
            print('SQL in [PyDBC.delete] : ' + sql)
        row_count = self.execute(sql, [list(conditions.values())])
        return row_count
