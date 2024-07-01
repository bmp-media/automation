import pandas as pd
import numpy as np
from modules.paths import PathManager
from modules.dataframe import DataframeManager
from modules.workbook import WorkbookManager
from modules import logics

def main() -> None:

    file_path = PathManager.open_file_dialog_solo()

    if not file_path:
        print('Файл не выбран.')
        return None

    df = DataframeManager.import_dataframe(file_path)
    df.dropna(axis='columns', how='all', inplace=True)

    # Целевая аудитория
    target = df.iloc[0, df.shape[1] - 1]
    
    # Удаляем пустые столбцы
    df.drop(df.columns[[-1, -2, -7, -8]], axis=1, inplace=True)

    # Определяем заголовок
    header = list(df.iloc[0, :-4])
    header.extend(['Sales TVR', 'Std. Sales TVR (20)', f'{target} TVR', f'{target} Std. TVR (20)'])
    df.columns = header

    # Сбрасываем индексацию, удаляем лишние строки
    df.drop([0, 1], axis=0, inplace=True)
    df.reset_index(drop=True, inplace=True)

    for column in df.columns[-4:]:
        df[column] = df[column].apply(lambda x: str(x).replace(',', '.')).astype('float64')

    df['Ролик ожидаемая длительность'] = df['Ролик ожидаемая длительность'].astype('int64')
    df['Ролик ID'] = df['Ролик ID'].astype('int64')

    if 'Ролик ID выхода оригинала' in df.columns:
        df['Ролик ID выхода оригинала'] = df['Ролик ID выхода оригинала'].astype('int64')
    
    if 'Блок ID выхода' in df.columns:
        df['Блок ID выхода'] = df['Блок ID выхода'].astype('int64')

    # Импортируем файл для обработки даты (CSV UTF-8)
    date = pd.read_csv(fr'addons/dates.csv', encoding='utf-8', sep=';')
    date.dropna(axis='columns', how='all', inplace=True) # Удаляем пустые столбцы

    # Соединяем по первичному ключу (LEFT JOIN)
    df['Дата'] = df['Дата'].apply(lambda x: str(x).replace('/', '.'))
    df = df.merge(date, how='left', on='Дата')

    # Определяем временные столбцы
    df['Время начала (0-24)'] = df['Ролик время начала'].apply(logics.get_correct_time)
    df['Час начала'] = df['Время начала (0-24)'].apply(lambda x: int(x[:2]))
    df['День / Ночь'] = np.where((df['Час начала'] >= 2) & (df['Час начала'] <= 4), 'Ночь', 'День')
    df['Временной интервал'] = df['Время начала (0-24)'].apply(logics.get_time_interval)


    # Импортируем файл для сопоставления типа телеканала (CSV UTF-8)
    ad = pd.read_csv(fr'addons/ad_channels.csv', encoding='utf-8', sep=';')
    ad.dropna(axis='columns', how='all', inplace=True) # Удаляем пустые столбцы

    # Определяем тип телеканала
    df['Тип канала'] = df['Телекомпания'].apply(lambda x: logics.get_channel_type(x, ad))

    # Импортируем файл для установки границ Prime (CSV UTF-8)
    off_prime = pd.read_csv(fr'addons/off_prime.csv', encoding='utf-8', sep=';')
    off_prime.dropna(axis='columns', how='all', inplace=True) # Удаляем пустые столбцы

    df = df.merge(off_prime, how='left', on=['Тип канала', 'День тип'])

    # Удаляем из каналов скобки
    df['Телекомпания'] = df['Телекомпания'].apply(logics.get_cleared_channel)

    # Премиальное / Средний
    df['Позиционирование'] = df['Ролик позиционирование '].apply(lambda x: 'Средний' if x == 'Средний' else 'Премиальное')

    # Импортируем файл для определения баинговой аудитории (CSV UTF-8)
    auditory = pd.read_csv(fr'addons/auditory.csv', encoding='utf-8', sep=';')
    auditory.dropna(axis='columns', how='all', inplace=True) # Удаляем пустые столбцы

    # Соединяем по первичному ключу (LEFT JOIN)
    df = df.merge(auditory, how='left', on='Телекомпания')

    # Импорт файл для определения телеканала и его номера
    channels = pd.read_csv(fr'addons/channels.csv', encoding='utf-8', sep=';')
    channels.dropna(axis='columns', how='all', inplace=True) # Удаляем пустые столбцы

    # Соединяем по первичному ключу (LEFT JOIN)
    df = df.merge(channels, how='left', on=['Ролик распространение', 'Телекомпания'])

    # Проставляем прайм
    df['от'] = np.where(pd.to_timedelta(df['Время начала (0-24)']) > pd.to_timedelta(df['Off от']), 1, 0)
    df['до'] = np.where(pd.to_timedelta(df['Время начала (0-24)']) <= pd.to_timedelta(df['Off до']), 1, 0)
    df['от_до'] = df['от'] * df['до']
    df['Prime'] = np.where(df['от_до'] == 1, 'OFF', 'PT')

    # Удаляем временные столбцы
    del df['от']
    del df['до']
    del df['от_до']

    # Импорт файла для округления (CSV UTF-8)
    round = pd.read_csv(fr'addons/round.csv', encoding='utf-8', sep=';')
    round.dropna(axis='columns', how='all', inplace=True) # Удаляем пустые столбцы

    df = df.merge(round, how='left', on=['Год', 'Ролик распространение', 'Телекомпания'])

    # Добавляем округление
    df['Округление'] = df['Округление'].apply(lambda x: str(x).replace(',','.').strip()).astype('float64')
    df['GRP_округл'] = np.max(df [['Sales TVR', 'Округление']], axis = 1)
    df['GRP20_округл'] = (df['GRP_округл'] * df['Ролик ожидаемая длительность']) / 20
    df['GRP20/min'] = np.where(df['Ролик тип'] == 'Ролик', df['GRP20_округл'], df ['Ролик ожидаемая длительность'] / 60)

    df = df.round(4)
    
    wb = WorkbookManager.create_workbook()
    ws = WorkbookManager.create_sheet(wb, 'Выгрузка')

    DataframeManager.export_dataframe_to_sheet(df, ws)

    # Форматирование Excel книги
    WorkbookManager.format_sheet(ws)

    # Сохраняем Excel книгу
    file_path_to_save = PathManager.save_file_dialog()

    if file_path_to_save:
        WorkbookManager.save_workbook(wb, file_path_to_save)
        print(f'Файл успешно сохранен: {file_path_to_save}')
    else:
        print('Файл не сохранен.')


if __name__ == '__main__':
    main()
