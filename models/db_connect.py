import sqlite3
import pandas as pd

path_to_sp_db = '/home/valentin-rexer/uni/UofM/datascience/datasets/swissprot.dat.db'
path_to_sep_ec_db = '/home/valentin-rexer/uni/UofM/datascience/datasets/ec_sep_swissprot.dat.db'


def read_db_to_df(ec_type='"%"', table_name='sw_table',):
    return pd.read_sql_query(f'SELECT EC_Number, Sequence FROM {table_name} WHERE EC_Number LIKE {ec_type}', sqlite3.connect(path_to_sp_db))


def read_sep_ec_db_to_df(ec_1, ec_2=-1, ec_3=-1, ec_4=-1):
    if ec_2 == -1 and ec_3 == -1 and ec_4 == -1:
        return pd.read_sql_query(
            f'SELECT * FROM sep_ec_table WHERE EC1 == {ec_1}',
sqlite3.connect(path_to_sep_ec_db)
        )

    elif ec_3 == -1 and ec_4 == -1:
        return pd.read_sql_query(
            f'SELECT * FROM sep_ec_table WHERE EC1 == {ec_1} AND EC2 == {ec_2}',
            sqlite3.connect(path_to_sep_ec_db)
        )

    elif ec_4 == -1:
        return pd.read_sql_query(
            f'SELECT * FROM sep_ec_table WHERE EC1 == {ec_1} AND EC2 == {ec_2} AND EC3 == {ec_3}',
            sqlite3.connect(path_to_sep_ec_db)
        )

    else:
        return pd.read_sql_query(
            f'SELECT * FROM sep_ec_table WHERE EC1 == {ec_1} AND EC2 == {ec_2} AND EC3 == {ec_3} AND EC4 == {ec_4}',
            sqlite3.connect(path_to_sep_ec_db)
        )


