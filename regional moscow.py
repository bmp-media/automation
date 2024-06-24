from modules.paths import PathManager
from modules.dataframe import DataframeManager
from modules.workbook import WorkbookManager
import pandas as pd
import numpy as np
import json

def main() -> None:
    file_path = PathManager.open_file_dialog_solo()

    if not file_path:
        print('Файл не выбран.')
        return None

    df = pd.read_csv(file_path, decimal=',', header=None, encoding='cp1251', sep='\t')

    # Drop empty columns and rows
    df.dropna(axis='columns', how='all', inplace=True)
    df.dropna(axis='index', how='all', inplace=True)

    # Extract header information directly from DataFrame
    header_info = df.iloc[:3, :].values

    # Process header information
    result = []
    for item in header_info.T:
        if not pd.isna(item[0]) and pd.isna(item[1]) and pd.isna(item[2]):
            result.append(item[0].strip())
        elif 'BA' in item[0]:
            item[0] = item[0].strip('BA').strip(' ')
            result.append(f'{item[0]} {item[2]}')
        else:
            result.append(f'{item[0]} {item[2]}')

    # Assign cleaned headers to DataFrame
    df.columns = result

    # Drop header rows and reset index
    df = df.drop([0, 1, 2], axis='index').reset_index(drop=True)

    # Clean and convert columns related to 'TVR'
    tvr_columns = [i for i in df.columns if 'TVR' in i]
    df.loc[:, tvr_columns] = df.loc[:, tvr_columns].replace({',': '.', '\\s+': ''}, regex=True)
    df.loc[:, tvr_columns] = df.loc[:, tvr_columns].astype('float64')

    # Clean and convert specified columns to int64
    int_columns = ['Ролик ID', 'Ролик ожидаемая длительность', 'Ролик ID выхода', 'Блок ID выхода']
    df.loc[:, int_columns] = df.loc[:, int_columns].replace({',': '.', '\\s+': ''}, regex=True).astype('int64')

    # Import data
    with open(r'round.json', encoding="utf-8") as file:
        round_df = file.read()
    round_df = pd.DataFrame(json.loads(round_df))

    with open(r'channels.json', encoding="utf-8") as file:
        channels_data = file.read()
    channels_data = json.loads(channels_data)
    nums = pd.DataFrame(channels_data['Номера'])
    auditory = pd.DataFrame(channels_data['Аудитории'])

    with open(r'dates.json', encoding="utf-8") as file:
        dates = file.read()
    dates = pd.DataFrame(json.loads(dates))

    with open(r'prime.json', encoding="utf-8") as file:
        prime = file.read()
    prime = pd.DataFrame(json.loads(prime))
    
    #round_df = pd.DataFrame(json.load(open('round.json', errors='ignore')))
    #channels_data = json.load(open('channels.json', errors='ignore'))

    #dates = pd.DataFrame(json.load(open('dates.json', errors='ignore')))
    #prime = pd.DataFrame(json.load(open('prime.json', errors='ignore')))

    # Process date column
    df.loc[:, 'Дата'] = df.loc[:, 'Дата'].apply(lambda x: str(x).replace('/', '.'))

    # Merge dataframes
    df = df.merge(dates, how='left', on='Дата')
    df = df.merge(auditory, how='left', on='Телекомпания оригинала')
    df = df.merge(nums, how='left', on='Телеканал')

    # Additional processing
    df['Город'] = df['Регион']
    df['Регион'] = df['Регион'].apply(lambda x: 'Москва' if x == 'МОСКВА' else 'Регионы')

    df['Телеканал'] = df['Телеканал'].str.upper()
    prime['Телеканал'] = prime['Телеканал'].str.upper()

    df = df.merge(prime, how='left', on=['Регион', 'Телеканал', 'Тип'])

    # Time processing function
    def adjust_time(time):
        hours, minsAndSecs = time[:2], time[-6:]
        hours = str((int(hours) % 24)).zfill(2)
        time_new = hours + minsAndSecs
        return hours, time_new

    # Apply adjust_time function to 'Ролик время начала' column
    df['Час начала'], df['Время начала (0-24)'] = zip(*df['Ролик время начала'].apply(adjust_time))

    # Convert columns to timedelta
    df['Время начала (0-24)'] = pd.to_timedelta(df['Время начала (0-24)'])
    df['Off от'] = pd.to_timedelta(df['От'])
    df['Off до'] = pd.to_timedelta(df['До'])

    # Convert 'Час начала' column to numeric format
    df['Час начала'] = pd.to_numeric(df['Час начала'])

    # Set values for 'Prime' column
    df['Prime'] = np.where((df['Время начала (0-24)'] > df['Off от']) & (df['Время начала (0-24)'] <= df['Off до']), 'OFF', 'PT')

    # Set values for 'День/Ночь' column
    df['День/Ночь'] = np.where((df['Час начала'] >= 2) & (df['Час начала'] <= 4), 'Ночь', 'День')

    # Drop unnecessary columns
    df.drop(['от', 'до', 'от_до'], axis=1, errors='ignore', inplace=True)

    # Merge with round dataframe
    df = df.merge(round_df, how='left', on=['Год', 'Телеканал'])

    # Process TVR columns
    tvr_df = df.loc[:, [i for i in df.columns if ('TVR' in i and 'Stand' not in i)]]
    tvr_df.columns = [i.replace(' TVR', '') for i in tvr_df.columns]
    tvr_df['Аудитория'] = df['Аудитория']
    tvr_df['минуты'] = 0
    df['Reg. Sales TVR'] = tvr_df.apply(lambda row: row[row['Аудитория']], axis=1)
    tvr_df = tvr_df.drop(['Аудитория', 'минуты'], axis='columns')

    # Process Stand TVR columns
    stand_tvr_df = df.loc[:, [i for i in df.columns if ('TVR' in i and 'Stand' in i)]]
    stand_tvr_df.columns = [i.replace(' Stand. TVR (20)', '') for i in stand_tvr_df.columns]
    stand_tvr_df['Аудитория'] = df['Аудитория']
    stand_tvr_df['минуты'] = 0
    df['Reg. Stand TVR'] = stand_tvr_df.apply(lambda row: row[row['Аудитория']], axis=1)
    stand_tvr_df = stand_tvr_df.drop(['Аудитория', 'минуты'], axis='columns')

    # Set values for 'Reg. TVR All 18+' and 'Reg. Stand. TVR All 18+'
    df['Reg. TVR All 18+'] = df['All 18+ TVR']
    df['Reg. Stand. TVR All 18+'] = df['All 18+ Stand. TVR (20)']

    # Cleaning and conversion
    df['Округление'] = df['Округление'].apply(lambda x: str(x).replace(',', '.').strip()).astype('float64')

    # Set values for 'Reg. GRP_округл'
    df['Reg. GRP_округл'] = np.max(df[['Reg. Sales TVR', 'Округление']], axis=1)

    # Calculate 'Reg. GRP20_округл'
    df['Reg. GRP20_округл'] = df['Reg. GRP_округл'] * df['Ролик ожидаемая длительность'] / 20

    # Set values for 'check Sales TVR' column
    df['check Sales TVR'] = df['Аудитория'].apply(lambda x: 'no' if x == 'минуты' else 'yes')

    # Calculate 'Reg. GRP20/min' column
    df_list = list(df['check Sales TVR'])

    def minutes(args):
        index, element = args
        if element == 'no':
            return df['Ролик ожидаемая длительность'][index] / 60
        else:
            return df['Reg. GRP20_округл'][index]
    
    df['Reg. GRP20/min'] = list(map(minutes, enumerate(df_list)))

    # List of columns to be removed
    ba = ['All 18+ TVR', 'All 18+ Stand. TVR (20)', 'All 4-45 TVR', 'All 4-45 Stand. TVR (20)',
          'All 6-54 TVR', 'All 6-54 Stand. TVR (20)', 'All 10-45 TVR', 'All 10-45 Stand. TVR (20)',
          'All 11-34 TVR', 'All 11-34 Stand. TVR (20)', 'All 14-39 TVR', 'All 14-39 Stand. TVR (20)',
          'All 14-44 TVR', 'All 14-44 Stand. TVR (20)', 'All 14-54 TVR', 'All 14-54 Stand. TVR (20)',
          'All 14-59 TVR', 'All 14-59 Stand. TVR (20)', 'All 25-49 TVR', 'All 25-49 Stand. TVR (20)',
          'All 25-54 TVR', 'All 25-54 Stand. TVR (20)', 'All 25-59 TVR', 'All 25-59 Stand. TVR (20)',
          'W 14-44 TVR', 'W 14-44 Stand. TVR (20)', 'W 25-59 TVR', 'W 25-59 Stand. TVR (20)',
          'M 18+ TVR', 'M 18+ Stand. TVR (20)']

    del df['check Sales TVR']

    # List of columns to keep
    columns_to_keep = [i for i in df.columns if i not in ba]

    # Select the columns to keep
    df = df.loc[:, columns_to_keep]
    df = df.round(4)

    # Export dataframe to openpyxl
    wb = WorkbookManager.create_workbook()
    ws = WorkbookManager.create_sheet(wb, 'Выгрузка')
    DataframeManager.export_dataframe_to_sheet(df, ws)

    # Format Excel sheet
    WorkbookManager.format_sheet(ws)

    # Save Excel workbook
    file_path_to_save = PathManager.save_file_dialog()

    if file_path_to_save:
        WorkbookManager.save_workbook(wb, file_path_to_save)
        print(f'Файл успешно сохранен: {file_path_to_save}')
    else:
        print('Файл не сохранен.')

if __name__ == '__main__':
    main()
