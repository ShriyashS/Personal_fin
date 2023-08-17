from contextlib import contextmanager
from dotenv import load_dotenv
import logging
import psycopg2
import os
logging.basicConfig(level=logging.INFO)


#Getting enviroment values
load_dotenv()

#DB connection string
db_u = os.environ.get('DB_USER')
db_d = os.environ.get('DB_DATABASE')
db_h = os.environ.get('DB_HOST')
db_pr = os.environ.get('DB_PORT')
db_pw = os.environ.get('DB_PASSWORD')
conn_var = {'dbname':db_d,'user':db_u,'password':db_pw,'host':db_h,'port':db_pr}


@contextmanager
def get_postgres_conn():
    try:
        logging.info('INSIDE DB Connection Function')
        db_connection = psycopg2.connect(**conn_var)
        logging.info('SUCCESSFULLY DB CONNECTION ESTABLISHED')
        yield db_connection
    except psycopg2.OperationalError as e:
        logging.error(e)
    finally:
        db_connection.close()

# with get_postgres_conn(conn_var) as conn:
#     cur = conn.cursor()
#     cur.execute("SELECT 1+2")
#     print(cur.fetchone())
#     cur.close()

    