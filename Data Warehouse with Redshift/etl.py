import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ 
    The function to load data from S3 to staging tables on Redshift.
  
    Parameters: 
        cur (object): The cursor of sql connection used to execute SQL statement.
        conn (object): The sql connection to Redshift.
    """
    
    for query in copy_table_queries:
        try:
            cur.execute(query)
        except:
            print('Loading staging table error in query:' + query)
            
        conn.commit()


def insert_tables(cur, conn):
    """ 
    The function to load data from staging tables to analytics tables on Redshift.
  
    Parameters: 
        cur (object): The cursor of sql connection used to execute SQL statement.
        conn (object): The sql connection to Redshift.
    """
        
    for query in insert_table_queries:
        try:
            cur.execute(query)
        except:
            print('Inserting analytics table error in query:' + query)
            
        conn.commit()


def main():
    print('ETL started.')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except:
        print('Redshift conneciton error')
    
    load_staging_tables(cur, conn)
    print('Finish loading staging tables')
    insert_tables(cur, conn)
    print('Finigh inserting dimensional analytics tables')
    
    conn.close()
    print('ETL successfully done.')


if __name__ == "__main__":
    main()