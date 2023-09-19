import pandas as pd
from collections import Counter
import uuid


running_total = Counter()
# Set options #####
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_colwidth', None)
pd.set_option('mode.chained_assignment', None)


def calc_running_total(data: str) -> Counter:
    """
    This function is used for calculating running total of each word
    and storing the result in a dictionary
    :param data:
    :return dictionary:
    """
    global running_total
    running_total += Counter(data)
    return running_total


def gen_init_df_from_json(json_file: str) -> pd.DataFrame:
    """
    function for generating dataframe from the raw json file.
    :param json_file:
    :return: dataframe
    """
    try:
        pd_df = pd.read_json(json_file)
        return pd_df
    except Exception as ex:
        print(f"ERROR performing transformation: {ex}")
        raise ex


def perform_transform(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    This function is for performing data transformation.
    :param df_in:
    :return: transformed_dataframe
    """
    try:
        print('perform_transform STARTED')
        df_in['Text'] = df_in['abstract'].to_dict().values()
        df_in['running_total'] = df_in['Text'].apply(lambda x:  calc_running_total(x['_value'].replace('\n', ' ').lower().split(' ')))

        last_running_total_dict = df_in.to_dict('records')[-1]['running_total']

        pos = 1
        most_common_columns = []
        for key, value in sorted(last_running_total_dict.items(), key=lambda item: item[1], reverse=True):
            if len(key) >= 5 and pos <= 20:
                most_common_columns.append(key)
                pos += 1

        df = df_in[['Text', 'running_total']]
        df['petition_id'] = df['Text'].apply(lambda _ : uuid.uuid4().hex)

        # Add 20 most common words as field to dataframe.
        for column in most_common_columns:
            df[column] = df['Text'].apply(lambda x: x['_value'].replace('\n', ' ').lower().split(' ').count(column))

        df.drop(columns=['Text', 'running_total'], inplace=True)

        return df
    except Exception as ex:
        print(f"ERROR performing transformation: {ex}")
        # raise ex


def write_df_content_to_csv(df: pd.DataFrame) -> None:
    """
    Function to write the result of DataFrame to a CSV file without the index column
    :param df:
    :return:
    """
    df.to_csv('output.csv', index=False)


if __name__ == '__main__':
    init_df = gen_init_df_from_json('input_data.json')
    out_df = perform_transform(init_df)
    print(out_df)
    write_df_content_to_csv(out_df)

