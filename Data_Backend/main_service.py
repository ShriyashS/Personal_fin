from utils.bank_extracter import bank_dataframe_extraction
from utils.db_connector import get_postgres_conn
import hashlib
import logging
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)

def hashing_func(s : pd.Series) -> str:
    """Generate hashed values"""
    logging.info('INSIDE TRY HASHING FUNC')
    try:
        m = hashlib.sha256()
        con_row_str = str(s['TRANSACTION_DATE']) + str(s['REF_NO']) + str(s['TRANSACTION_DESC']) + \
            str(s['REF_DATE']) + str(s['DEBIT']) + str(s['CREDIT']) + str(s['BALANCE'])
        m.update(con_row_str.encode('utf-8'))
        return m.hexdigest()
    except Exception as e:
        logging.error(e)
        raise(e)
    
    

def state_bank_df()-> pd.DataFrame:
    """Fetch the data and generate final DF"""
    logging.info('INSIDE BANK DF GENERATOR')
    bank_obj = bank_dataframe_extraction()
    input_list_dfs = bank_obj.extract_info()
    output_df = bank_obj.data_processing(input_list_dfs)
    output_df['UUID_ROW'] = output_df.apply (lambda row: hashing_func(row), axis=1)
    output_df.insert(0, 'B_ID', range(0,len(output_df)))
    return output_df

def data_push_db() -> str:
    """Push the data to POSTGRES"""
    with get_postgres_conn() as conn:
        table_create_sql = '''CREATE TABLE IF NOT EXISTS bank_statement (
            B_ID int PRIMARY KEY,
            TRANSACTION_DATE date NOT NULL,
            REF_DATE date NOT NULL,
            TRANSACTION_DESC varchar,
            REF_NO varchar,
            DEBIT numeric,
            CREDIT numeric,
            BALANCE numeric,
            UUID_ROW varchar UNIQUE NOT NULL);
        '''
        insert_sql = r'''
        COPY bank_statement 
        FROM 'C:\IT\Project\Personal\Personal_Financial_Dashboard\Data_Backend\bank_csv.csv'
        DELIMITER ',' CSV;
        '''
        cur = conn.cursor()
        # cur.execute('DROP TABLE bank_statement')
        cur.execute(table_create_sql)
        bank_df = state_bank_df()
        bank_df.to_csv('bank_csv.csv', index=False, header=False)
        cur.execute(insert_sql)
        conn.commit()
        cur.close()
    return 'success'

if __name__ == '__main__':
    s = data_push_db()
    print(s)