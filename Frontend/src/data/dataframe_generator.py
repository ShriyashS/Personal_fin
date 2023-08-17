import numpy as np
import pandas as pd
from src.data.db_connector import get_postgres_conn
# from db_connector import get_postgres_conn
#Get Total balance
def balance() -> str:
    """To get the Remainig Balance"""
    with get_postgres_conn() as conn:
        balance_sql = '''select balance from bank_statement order by b_id desc limit 1;
        '''
        cur = conn.cursor()
        # cur.execute('DROP TABLE bank_statement')
        cur.execute(balance_sql)
        value = cur.fetchone()
        cur.close()
    return value[0]

#Get Total Credit
def total_cred_balance() -> str:
    """To get the Remainig Balance"""
    with get_postgres_conn() as conn:
        cred_sql = '''select sum(credit) AS total_credit from bank_statement;
        '''
        cur = conn.cursor()
        # cur.execute('DROP TABLE bank_statement')
        cur.execute(cred_sql)
        value = cur.fetchone()
        cur.close()
    return value[0]

#Get Total Credit
def total_deb_balance() -> str:
    """To get the Remainig Balance"""
    with get_postgres_conn() as conn:
        deb_sql = '''select sum(debit) AS total_credit from bank_statement;
        '''
        cur = conn.cursor()
        # cur.execute('DROP TABLE bank_statement')
        cur.execute(deb_sql)
        value = cur.fetchone()
        cur.close()
    return value[0]

#Get table
def total_all_data() -> pd.DataFrame:
    """To get the Remainig Balance"""
    with get_postgres_conn() as conn:
        df = pd.read_sql_query('select * from bank_statement;',con=conn)
    return df
