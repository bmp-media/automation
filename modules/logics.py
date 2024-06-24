import pandas as pd
import re


class ProcessingManager:

    @staticmethod
    def affinity(df: pd.DataFrame):

        try:
            df.drop(df.columns[[0, 3, 4, 7]], axis=1, inplace=True)
            df.reset_index(inplace=True, drop=True)

            target = df.iloc[0, 3]
            
            df.drop([0, 1], axis=0, inplace=True)

            df.columns = ['Блок распространения', 'Телеканал', 'Sales TVR', f'{target} TVR']

            df[f'{target} TVR'] = df[f'{target} TVR'].apply(lambda x: str(x).replace(',', '.')).astype('float64')
            df['Sales TVR'] = df['Sales TVR'].apply(lambda x: str(x).replace(',', '.')).astype('float64')

            df[f'Affinity {target}'] = df[f'{target} TVR'] / df['Sales TVR']

            df['Телеканал'] = df['Телеканал'].apply(get_cleared_channel)

            df = df.round(4)

            return {
                'dataframe': df,
                'target': target
            }

        except Exception as e:
            raise Exception('Ошибка при обработке датафрейма') from e
        

    @staticmethod
    def tvr(df: pd.DataFrame):

        try:
            df.drop(df.columns[[0, 4]], axis=1, inplace=True)

            # Определяем целевую аудиторию
            target = df.iloc[0, 3]

            # Переименовываем столбцы
            df.columns = ['Блок распространения', 'Телеканал', 'PT / OP', target]
            df.drop([0, 1], axis=0, inplace=True)

            # Форматируем телеканалы и определяем формат чисел
            df['Телеканал'] = df['Телеканал'].apply(lambda x: get_cleared_channel(x))
            df[target] = df[target].apply(lambda x: str(x).replace(',', '.')).astype('float64')

            # Округляем значения до 4 знаков
            df = df.round(4)

            return {
                'dataframe': df,
                'target': target
            }

        except Exception as e:
            raise Exception('Ошибка при обработке датафрейма') from e


    @staticmethod
    def reach(df: pd.DataFrame):
        
        REMAINS = 50

        try:
            df.drop(df.columns[[0, 3]], axis=1, inplace=True)

            target = df.iloc[0, 2]
            header = ['Рекламодатель', 'Дата', f'{target} TVR']

            for i in range(3, len(df.columns)):
                header.append(df.iloc[2, i])

            df.columns = header

            df.drop([0, 1, 2], axis=0, inplace=True)
            df.reset_index(drop=True, inplace=True)

            for column in header[2:]:
                df[column] = df[column].apply(lambda x: x.replace(',', '.')).astype('float64')

            df['Дата'] = df['Дата'].apply(lambda x: x.replace('/', '.'))

            df['Total TVR'] = None

            df.loc[0, 'Total TVR'] = df.loc[0, f'{target} TVR']

            for i in range(1, len(df['Total TVR'])):
                    df.loc[i, 'Total TVR'] = df.loc[i, f'{target} TVR'] + df.loc[i - 1, 'Total TVR']
                    

            df['Остаток'] = df['Total TVR'] % REMAINS
            df['Шаг'] = df['Total TVR'] - df['Остаток']

            df['Отсечка'] = None

            for i in range(1, len(df['Остаток'])):
                    df.loc[i, 'Отсечка'] = 'yes' if df.loc[i - 1, 'Остаток'] > df.loc[i, 'Остаток'] else 'no'

            df.loc[len(df) - 1, 'Отсечка'] = 'yes'

            # Округляем значения до 4 знаков
            df = df.round(4)

            return {
                'dataframe': df,
                'target': target
            }

        except Exception as e:
            raise Exception('Ошибка при обработке датафрейма') from e


    @staticmethod
    def positioning(df: pd.DataFrame):
        
        try:
            df.drop(df.columns[[0, 4]], axis=1, inplace=True)

            # Определяем целевую аудиторию
            target = df.iloc[0, 3]

            # Переименовываем столбцы
            df.columns = ['Блок распространения', 'Телеканал', 'Позиция', target]
            df.drop([0, 1], axis=0, inplace=True)

            # Форматируем телеканалы и определяем формат чисел
            df['Телеканал'] = df['Телеканал'].apply(lambda x: get_cleared_channel(x))
            df[target] = df[target].apply(lambda x: str(x).replace(',', '.').replace('\xa0', '')).astype('float64')

            # Округляем значения до 4 знаков
            df = df.round(4)

            return {
                'dataframe': df,
                'target': target
            }

        except Exception as e:
            raise Exception('Ошибка при обработке датафрейма') from e


def get_cleared_channel(string: str) -> str:
    '''Возвращает строку с телеканалом без скобок и значений в них'''
    return re.sub(r'\([^()]*\)', '', string).strip()


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
    if telecompany_name in df['Union']:
        return 'ЕРК'
    elif telecompany_name in df['Digital']:
        return 'ЦРК'
    else:
        return 'Нац'