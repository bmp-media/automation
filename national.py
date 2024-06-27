from modules.paths import PathManager
from modules.dataframe import DataframeManager
from modules.workbook import WorkbookManager
from modules.logics import get_cleared_channel
import pandas as pd
import numpy as np


def main() -> None:

    file_path = PathManager.open_file_dialog_solo()

    if not file_path:
        print('Файл не выбран.')
        return None

    df = DataframeManager.import_dataframe(file_path)
    df.dropna(axis='columns', how='all', inplace=True)

    # Избавляет от NaN в первых трех строках
    try:
        df.iloc[0] = df.iloc[0].apply(lambda x: '' if x == 'Consolidated' else x)
        df.iloc[1] = df.iloc[1].apply(lambda x: '' if x is np.NAN else x)
        df.iloc[2] = df.iloc[2].apply(lambda x: '' if x is np.NAN else x)
    except:
        df.iloc[0] = df.iloc[0].apply(lambda x: '' if x == 'Consolidated' else x)
        df.iloc[1] = df.iloc[1].apply(lambda x: '' if x is np.nan else x)
        df.iloc[2] = df.iloc[2].apply(lambda x: '' if x is np.nan else x)

    # Конкатенирует строки в заголовок
    header = [str(i + j).strip() for i, j in zip(df.iloc[0], (df.iloc[1] + ' ' + df.iloc[2]))]
    df.columns = header

    # Удаляет лишние строки и сбрасываем индексацию
    df.drop([0, 1, 2, 3], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Список столбцов с типом данных float64
    substrings = ('desktop', 'tv', 'mobile', 'id', 'длительность')

    for column in df.columns:
        if is_substring(substrings, column):
            df[column] = df[column].apply(lambda x: str(x).replace(',', '.').strip()).astype('float64')

    # Приводит дату в формате dd/mm/yy к dd.mm.yy
    df['Дата'] = df['Дата'].apply(lambda x: x.replace('/', '.'))

    # Импорт дат (CSV UTF-8)
    date = pd.read_csv('addons/dates.csv',encoding='utf-8', sep=';')
    date.dropna(axis='columns', how='all', inplace=True)

    # LEFT JOIN
    df = df.merge(date, how='left', on='Дата')

    # Определяет временные столбцы
    df['Время начала (0-24)'] = df['Ролик время начала'].apply(get_correct_time)
    df['Час начала'] = df['Время начала (0-24)'].apply(lambda x: int(x[:2]))
    df['День / Ночь'] = np.where((df['Час начала'] >= 2) & (df['Час начала'] <= 4), 'Ночь', 'День')
    df['Временной интервал'] = df['Время начала (0-24)'].apply(get_time_interval)

    # Импорт телеканалов (CSV UTF-8)
    ad = pd.read_csv('addons/ad_channels.csv',encoding='utf-8', sep=';')
    ad.dropna(axis='columns', how='all', inplace=True)

    # Определяет тип телеканала
    df['Тип канала'] = df['Телекомпания'].apply(lambda x: get_channel_type(x, ad))

    # Импортируем файл для установки границ Prime (CSV UTF-8)
    off_prime = pd.read_csv('addons/off_prime.csv', encoding='utf-8', sep=';')

    # Удаляем пустые столбцы
    off_prime.dropna(axis='columns', how='all', inplace=True)

    df = df.merge(off_prime, how='left', on=['Тип канала', 'День тип'])

    # Платформа оригинала
    df['Платформа оригинала'] = df['Телекомпания'].apply(lambda x: 'ТВ' if 'СЕТЕВОЕ' in x else 'Интернет')

    # Удаляем из каналов скобки
    df['Телекомпания'] = df['Телекомпания'].apply(get_cleared_channel)

    # Премиальное / Средний
    df['Позиционирование'] = df['Ролик позиционирование'].apply(lambda x: 'Средний' if x == 'Средний' else 'Премиальное')

    # Импортируем файл для определения баинговой аудитории (CSV UTF-8)
    auditory = pd.read_csv('addons/auditory.csv', encoding='utf-8', sep=';')

    # Удаляем пустые столбцы
    auditory.dropna(axis='columns', how='all', inplace=True)

    # Соединяем по первичному ключу (LEFT JOIN)
    df = df.merge(auditory, how='left', on='Телекомпания')

    # Импорт файл для определения телеканала и его номера
    channels = pd.read_csv('addons/channels.csv', encoding='utf-8', sep=';')
    # Удаляем пустые столбцы
    channels.dropna(axis='columns', how='all', inplace=True)

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

    # Мэтчим строки Интернет и ТВ
    df_group = df[[column for column in df.columns if is_rating(column) or column == 'Ролик ID выхода оригинала']]
    df_group = df_group.groupby('Ролик ID выхода оригинала').sum()

    df = df[[column for column in df.columns if not is_rating(column)]]
    df = df[df['Платформа оригинала'] == 'ТВ']
    del df['Платформа оригинала']

    df = df.merge(df_group, how='left', on='Ролик ID выхода оригинала')

    # Sales TVR TV
    tv = df[[column for column in df.columns if 'TV BA' in column]]
    tv.columns = [column.replace('TV ', '') for column in tv.columns]

    tv.insert(0, 'Баинговая аудитория', df['Баинговая аудитория'])
    df['Sales TVR TV'] = tv.apply(lambda x: x[x['Баинговая аудитория']], axis=1)

    # Sales TVR Desktop
    desktop = df[[column for column in df.columns if 'Desktop BA' in column]]
    desktop.columns = [column.replace('Desktop ', '') for column in desktop.columns]

    desktop.insert(0, 'Баинговая аудитория', df['Баинговая аудитория'])
    df['Sales TVR Desktop'] = desktop.apply(lambda x: x[x['Баинговая аудитория']], axis=1)

    # Sales TVR Mobile
    mobile = df[[column for column in df.columns if 'Mobile BA' in column]]
    mobile.columns = [column.replace('Mobile ', '') for column in mobile.columns]

    mobile.insert(0, 'Баинговая аудитория', df['Баинговая аудитория'])
    df['Sales TVR Mobile'] = mobile.apply(lambda x: x[x['Баинговая аудитория']], axis=1)

    # Sales Std. TVR
    df['Sales Std. TVR TV'] = (df['Sales TVR TV'] * df['Ролик ожидаемая длительность']) / 20
    df['Sales Std. TVR Desktop'] = (df['Sales TVR Desktop'] * df['Ролик ожидаемая длительность']) / 20
    df['Sales Std. TVR Mobile'] = (df['Sales TVR Mobile'] * df['Ролик ожидаемая длительность']) / 20

    # Big Sales TVR
    df['Big Sales TVR'] = df['Sales TVR TV'] + df['Sales TVR Desktop'] + df['Sales TVR Mobile']
    df['Big Sales Std. TVR'] = df['Sales Std. TVR TV'] + df['Sales Std. TVR Desktop'] + df['Sales Std. TVR Mobile']

    # Убираем лишние столбцы
    df = df[[i for i in df.columns if 'BA' not in i]]

    # Определяем целевые аудитории
    targets = set([i.replace('TV ', '') for i in df.columns if (('TV' in i) and ('TVR' not in i))])

    # TVR + Std. TVR + Big TVR
    for target in targets:
        df[f'TVR {target} TV'] = df[f'TV {target}']
        df[f'TVR {target} Desktop'] = df[f'Desktop {target}']
        df[f'TVR {target} Mobile'] = df[f'Mobile {target}']

        del df[f'TV {target}']
        del df[f'Desktop {target}']
        del df[f'Mobile {target}']

        df[f'Std. TVR {target} TV'] = (
            df[f'TVR {target} TV'] * df['Ролик ожидаемая длительность']) / 20
        df[f'Std. TVR {target} Desktop'] = (
            df[f'TVR {target} Desktop'] * df['Ролик ожидаемая длительность']) / 20
        df[f'Std. TVR {target} Mobile'] = (
            df[f'TVR {target} Mobile'] * df['Ролик ожидаемая длительность']) / 20

        df[f'Big TVR {target}'] = df[f'TVR {target} TV'] + \
            df[f'TVR {target} Desktop'] + df[f'TVR {target} Mobile']
        df[f'Big Std. TVR {target}'] = df[f'Std. TVR {target} TV'] + \
            df[f'Std. TVR {target} Desktop'] + df[f'Std. TVR {target} Mobile']

    # Импорт файла для округления (CSV UTF-8)
    round = pd.read_csv('addons/round.csv', encoding='utf-8', sep=';')
    # Удаляем пустые столбцы
    round.dropna(axis='columns', how='all', inplace=True)

    df = df.merge(round, how='left', on=['Год', 'Ролик распространение', 'Телекомпания'])

    # Добавляем округление
    df['Округление'] = df['Округление'].apply(lambda x: x.replace(',', '.').strip()).astype('float64')
    df['GRP_округл'] = np.max(df[['Big Sales TVR', 'Округление']], axis=1)
    df['GRP20_округл'] = (df['GRP_округл'] * df['Ролик ожидаемая длительность']) / 20
    df['GRP20/min'] = np.where(df['Ролик тип'] == 'Ролик', df['GRP20_округл'], df['Ролик ожидаемая длительность'] / 60)

    df['Кампания'] = None
    df['Размещение'] = None

    df = df.round(4)

    # Экспорт датафрейма в openpyxl
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


def is_substring(substrings: tuple, string: str) -> bool:
    '''Возвращает True, если столбец необходимо преобразовать в 'float64', иначе False'''
    return any(substring in string.lower() for substring in substrings)


def get_time_interval(time: str) -> str:
    '''Возвращает интервал времени'''
    hours = int(time[:2])

    intervals = {
        (6, 8): '06:00:00 - 09:00:00',
        (9, 12): '09:00:00 - 13:00:00',
        (13, 15): '13:00:00 - 16:00:00',
        (16, 18): '16:00:00 - 19:00:00',
        (19, 21): '19:00:00 - 22:00:00',
        (22, 23): '22:00:00 - 00:00:00',
        (0, 5): '00:00:00 - 06:00:00'
    }

    for interval, result in intervals.items():
        if interval[0] <= hours <= interval[1]:
            return result


def get_correct_time(time: str) -> str:
    '''Formats 29:00:00 hour format to 24:00:00'''
    hours, minutes, seconds = map(int, time.split(':'))

    hours %= 24

    return f'{hours:02}:{minutes:02}:{seconds:02}'


def get_channel_type(telecompany: str, df: pd.DataFrame) -> str:
    '''Возвращает тип телеканала'''
    telecompany_name = telecompany.replace('(СЕТЕВОЕ ВЕЩАНИЕ)', '').strip()
    if telecompany_name in df['Union'].to_list():
        return 'ЕРК'
    elif telecompany_name in df['Digital'].to_list():
        return 'ЦРК'
    else:
        return 'Нац'


def is_rating(column: str) -> bool:
    '''Возвращает True если это столбец с рейтингом, иначе False'''
    return any(device in column for device in ['TV', 'Mobile', 'Desktop'])


if __name__ == '__main__':
    main()
