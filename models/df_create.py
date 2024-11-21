import pandas
import pandas as pd
from db_connect import *

EC_LEVEL_1_HIGHEST = 7
EC_LEVEL_2_HIGHEST = 99
EC_LEVEL_3_HIGHEST = 99
EC_LEVEL_4_HIGHEST = 438

def get_ec_dataframe(first,second=0,third=0, fourth=0):

    """
    Returns all entries with specified EC_Numbers from the big database (==> more information, EC_Numbers not seperated)
    :param first:
    :param second:
    :param third:
    :param fourth:
    :return:
    """

    if second == 0 and third == 0 and fourth == 0:
        df = read_db_to_df(f'"{first}.%"')

    elif third == 0 and fourth == 0:
        df = read_db_to_df(f'"{first}.{second}.%"')

    elif fourth == 0:
        df = read_db_to_df(f'"{first}.{second}.{third}.%"')

    else:
        df = read_db_to_df(f'"{first}.{second}.{third}.{fourth}"')

    return df


def get_df_for_level(ec_1=-1, ec_2=-1, ec_3=-1, ec_4=-1):
    """
    Get all entries with specified EC numbers: if no number is specified, you get all entries
    :param ec_1:
    :param ec_2:
    :param ec_3:
    :param ec_4:
    :return: pandas.DataFrame with all rows that fulfill the given specifications
    """

    dfs = []

    if ec_1 == -1 and ec_2 == -1 and ec_3 == -1 and ec_4 == -1:
        for ec_class1 in range(1, EC_LEVEL_1_HIGHEST + 1):
            dfs.append(read_sep_ec_db_to_df(ec_class1))

    elif ec_2 == -1 and ec_3 == -1 and ec_4 == -1:
        for ec_class2 in range(0, EC_LEVEL_2_HIGHEST + 1):
            dfs.append(read_sep_ec_db_to_df(ec_1, ec_class2))

    elif ec_3 == -1 and ec_4 == -1:
        for ec_class3 in range(0, EC_LEVEL_3_HIGHEST + 1):
            dfs.append(read_sep_ec_db_to_df(ec_1, ec_2, ec_class3))

    elif ec_4 == -1:
        for ec_class4 in range(0, EC_LEVEL_4_HIGHEST + 1):
            dfs.append(read_sep_ec_db_to_df(ec_1, ec_2, ec_3, ec_class4))

    else:
        dfs.append(read_sep_ec_db_to_df(ec_1, ec_2, ec_3, ec_4))

    return pd.concat(dfs)




def get_single_class_df(ec_1=-1, ec_2=-1, ec_3=-1, ec_4=-1):
    df = get_df_for_level(ec_1, ec_2, ec_3, ec_4)
    columns = ["EC_Single_Class", "Sequence"]

    rows = []

    if ec_1 == -1 and ec_2 == -1 and ec_3 == -1 and ec_4 == -1:
        for entry in df.itertuples():
            row = [entry[1], entry[5]]
            rows.append(dict(zip(columns, row)))

    elif ec_2 == -1 and ec_3 == -1 and ec_4 == -1:
        for entry in df.itertuples():
            row = [entry[2], entry[5]]
            rows.append(dict(zip(columns, row)))

    elif ec_3 == -1 and ec_4 == -1:
        for entry in df.itertuples():
            row = [entry[3], entry[5]]
            rows.append(dict(zip(columns, row)))

    else:
        for entry in df.itertuples():
            row = [entry[4], entry[5]]
            rows.append(dict(zip(columns, row)))

    return pandas.DataFrame(rows)


print(get_single_class_df(1,3,4))