# загрузка библиотек медиаскопа
import sys
import os
import re
import json
import datetime
import time
import pandas as pd
import numpy as np
from modules.paths import PathManager
#from IPython.display import JSON

from mediascope_api.core import net as mscore
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc

import warnings
warnings.filterwarnings("ignore")

# Cоздаем объекты для работы с TVI API
mnet = mscore.MediascopeApiNetwork()
mtask = cwt.MediaVortexTask()
cats = cwc.MediaVortexCats()

# загружаем шаблоны

data = pd.read_excel(r'Шаблон.xlsx')
global channels
channels = pd.read_excel(r'Шаблон.xlsx', sheet_name='Каналы суперфикс')
global categories
categories = pd.read_excel(r'Шаблон.xlsx', sheet_name='Категории аффинити')
global pba
pba = pd.read_excel(data['PBA'][0], sheet_name='свод', skiprows=1)
global affinity_df
affinity_df = pd.read_excel('O:\\BMP Media Audit\\Media Research\\Проекты\\Оценка планирования\\Скрипты\\2. Оценка аффинити\\Пул кампаний с аффинити.xlsx')
global save_path
print('Укажи директорию для сохранения:\n')
save_path = PathManager.save_in_directory_dialog()
#диалоговое окно

pba = pba.rename(columns={'Клиент//Кампания': 'Кампания'})
data['Дата_старта'] = pd.to_datetime(data['Дата_старта']).dt.date
data['Дата_окончания'] = pd.to_datetime(data['Дата_окончания']).dt.date
affinity_df['Дата_старта'] = pd.to_datetime(affinity_df['Дата_старта']).dt.date
affinity_df['Дата_окончания'] = pd.to_datetime(affinity_df['Дата_окончания']).dt.date

# Функция для каналов
def get_cleared_channel(string: str) -> str:
    '''Возвращает строку с телеканалом без скобок и значений в них'''
    return re.sub(r'\([^()]*\)', '', string).strip()

# функция для расчёта рыночного аффинити
def affinity_market(row):
    # задаём стандартные условия для выгрузки из api
    # период
    l = list()
    l.append(str(row['Дата_старта']))
    l.append(str(row['Дата_окончания']))
    date_filter = [tuple(l)]
            
    # Фильтр роликов: статус - реальный, тип ролика - ролик, ролик распространение - сетевой или орбитальный
    ad_filter = 'adIssueStatusId = R and adTypeId = 1 and (adDistributionType = N or adDistributionType = O)'
            
    # Указываем список срезов (телекомпания и ролик распространение)
    slices = ['tvCompanyName', 'adDistributionTypeName']
            
    # Задаем опции расчета
    options = {
                "kitId": 1, #TV Index Russia all
                "issueType": "AD" #Тип события
    }

    # считаем sales tvr рынка
    statistics = ['SalesRtgPerSum'] # выбираем статистику sales tvr total
    print('Запускаем расчёт')
    task_json = mtask.build_crosstab_task(date_filter=date_filter, ad_filter=ad_filter, slices=slices, statistics=statistics, options=options)
    task_crosstab = mtask.wait_task(mtask.send_crosstab_task(task_json))
    df = mtask.result2table(mtask.get_result(task_crosstab))
    market_grp = df[slices+statistics]

    # считаем tvr рынка
    basedemo_filter = row['ЦА API'] # задаём ЦА
    statistics = ['RtgPerSum'] # выбираем статистику tvr total
    task_json = mtask.build_crosstab_task(date_filter=date_filter, basedemo_filter=basedemo_filter, ad_filter=ad_filter, 
                                            slices=slices, statistics=statistics, options=options)
    task_crosstab = mtask.wait_task(mtask.send_crosstab_task(task_json))
    df = mtask.result2table(mtask.get_result(task_crosstab))
    market_trp = df[slices+statistics]

    aff_market = market_trp.merge(market_grp, how='outer', on=['tvCompanyName', 'adDistributionTypeName'])
    aff_market.columns = ['Телекомпания', 'Ролик распространение', 'TRP_рынка', 'GRP_рынка']
    aff_market['Телекомпания'] = aff_market['Телекомпания'].apply(get_cleared_channel)
    
    return aff_market

# функция для расчёта рыночного праймтайма
def prime_time(row):
    # задаём стандартные условия для выгрузки из api
    # Задаем период
    l = list()
    l.append(str(row['Дата_старта']))
    l.append(str(row['Дата_окончания']))
    date_filter = [tuple(l)]
            
    # Задаем ЦА
    basedemo_filter = row['ЦА API']
        
    # Фильтр роликов: статус - реальный, тип ролика - ролик, ролик распространение - сетевой или орбитальный
    ad_filter = 'adIssueStatusId = R and adTypeId = 1 and (adDistributionType = N or adDistributionType = O)'
            
    # Указываем список срезов (телекомпания, ролик распространение, pt|op)
    slices = ['tvCompanyName', 'adDistributionTypeName', 'adPrimeTimeStatusName']
            
    # выбираем статистику tvr average
    statistics = ['RtgPerAvg']
            
    # Задаем опции расчета
    options = {
                "kitId": 1, #TV Index Russia all
                "issueType": "AD" #Тип события
    }

    # Формируем задание для API TV Index в формате JSON
    task_json = mtask.build_crosstab_task(date_filter=date_filter, basedemo_filter=basedemo_filter, ad_filter=ad_filter, 
                                            slices=slices, statistics=statistics, options=options)
    task_crosstab = mtask.wait_task(mtask.send_crosstab_task(task_json))
    df = mtask.result2table(mtask.get_result(task_crosstab))
    market_prime_time = df[slices+statistics]

    market_prime_time.columns = ['Телекомпания', 'Ролик распространение', 'Праймтайм', 'TRP рынок']
    market_prime_time['Телекомпания'] = market_prime_time['Телекомпания'].apply(get_cleared_channel)
    market_prime_time = market_prime_time.merge(channels, on=['Телекомпания', 'Ролик распространение'])

    trp_market_prime = market_prime_time[market_prime_time['Праймтайм'] == 'Прайм-тайм'][['Телеканал', 'Номер', 'TRP рынок']].reset_index()
    trp_market_prime = trp_market_prime.rename(columns={'TRP рынок':'TRP рынок PT'})

    trp_market_op = market_prime_time[market_prime_time['Праймтайм'] == 'Вне прайм-тайм'][['Телеканал', 'Номер', 'TRP рынок']].reset_index()
    trp_market_op = trp_market_op.rename(columns={'TRP рынок':'TRP рынок OP'})

    return trp_market_prime, trp_market_op

# функция для расчёта рыночного позиционирования
def premium_market(row):
    # Задаем период
    l = list()
    l.append(str(row['Дата_старта']))
    l.append(str(row['Дата_окончания']))
    date_filter = [tuple(l)]
    
    # Задаем ЦА
    basedemo_filter = row['ЦА API']
        
    # Фильтр роликов: статус - реальный, тип ролика - ролик, ролик распространение - сетевой или орбитальный
    ad_filter = 'adIssueStatusId = R and adTypeId = 1 and (adDistributionType = N or adDistributionType = O)'
                
    # Указываем список срезов (ролик распространение, телекомпания, позиционирование)
    slices = ['adDistributionTypeName', 'tvCompanyName', 'adPositionTypeName']
                
    # выбираем статистику tvr total
    statistics = ['RtgPerSum']
            
    # Задаем опции расчета
    options = {
                    "kitId": 1, #TV Index Russia all
                    "issueType": "AD" #Тип события
        }
    
    # Формируем задание для API TV Index в формате JSON
    print('Запускаем расчёт')
    task_json = mtask.build_crosstab_task(date_filter=date_filter, basedemo_filter=basedemo_filter, ad_filter=ad_filter, 
                                                slices=slices, statistics=statistics, options=options)
        
    # Отправляем задание на расчет и ждем выполнения
    task_crosstab = mtask.wait_task(mtask.send_crosstab_task(task_json))
    df = mtask.result2table(mtask.get_result(task_crosstab))
    df_premium = df[slices+statistics]

    return df_premium

#фильтруем стандартную выгрузку
def temp_dataframe(row, camp):
    main_data = pd.read_excel(row['Основная выгрузка'], sheet_name='Выгрузка')
    
    if pd.isnull(row['Убрать бонусы']):
        temp_df = main_data.query('Кампания == @camp')
    else:
        temp_df = main_data.query('Кампания == @camp and Размещение == "Основное"')

    return temp_df

#считаем аффинити кампании
def affinity_camp(temp_df, trgt):
    #аффинити кампании
    temp_affinity = temp_df.pivot_table(index=['Телеканал'], values=['Big TVR '+trgt, 'Big Sales TVR'], aggfunc=['sum']).reset_index()
    temp_affinity.columns = ['Телеканал', 'GRP_кампании', 'TRP_кампании']
    temp_affinity['Аффинити_кампании'] = temp_affinity['TRP_кампании']/temp_affinity['GRP_кампании']*100

    return temp_affinity

# оценка аффинити
def score_affinity(row):
    camp = row['Кампания']
    trgt = row['ЦА']
    temp_df = temp_dataframe(row, camp)
    temp_affinity = affinity_camp(temp_df, trgt)
    aff_market = affinity_market(row)
    affinity_score = aff_market.merge(categories, on=['Ролик распространение', 'Телекомпания'])
    affinity_score['Аффинити_рынка'] = affinity_score['TRP_рынка']/affinity_score['GRP_рынка']*100
    affinity_score = affinity_score[['Телеканал', 'Номер', 'Категория', 'TRP_рынка', 'GRP_рынка', 'Аффинити_рынка']]
    work = temp_affinity.merge(affinity_score, on='Телеканал')
    affinity = work[['Телеканал', 'Номер', 'Аффинити_кампании', 'Аффинити_рынка']].sort_values(by='Номер')

    # Расчёт долей по категориям каналов
    split = work.groupby('Категория')[['TRP_кампании', 'GRP_кампании']].sum().reset_index()
    split['Доля_TRP'] = split['TRP_кампании']/split['TRP_кампании'].sum()
    split['Доля_GRP'] = split['GRP_кампании']/split['GRP_кампании'].sum()
    
    # Расчёт индексов
    index = row[['Кампания', 'Дата_старта', 'Дата_окончания', 'ЦА']]
    index['Рекламодатель'] = row['Кампания'].split('/')[0]
    index['TRP_кампании'] = temp_affinity['TRP_кампании'].sum()
    index['GRP_кампании'] = temp_affinity['GRP_кампании'].sum()
    index['TRP_рынка'] = affinity_score['TRP_рынка'].sum()
    index['GRP_рынка'] = affinity_score['GRP_рынка'].sum()
    index['Affinity_кампании'] = index['TRP_кампании']/index['GRP_кампании']
    index['Affinity_рынка'] = index['TRP_рынка']/index['GRP_рынка']
    index['Индекс против рынка'] = index['Affinity_кампании']/index['Affinity_рынка']*100

    # оценка по нашему пулу
    adv = index['Рекламодатель']
    perc = 0.1
    max_trp = index['TRP_кампании'] + index['TRP_кампании']*perc
    min_trp = index['TRP_кампании'] - index['TRP_кампании']*perc
    data_sample = affinity_df.query('@min_trp <= TRP_кампании <= @max_trp and Рекламодатель != @adv')

    while len(data_sample) < 10 and perc <= 0.25:
        perc += 0.05
        max_trp = index['TRP_кампании'] + index['TRP_кампании']*perc
        min_trp = index['TRP_кампании'] - index['TRP_кампании']*perc
        data_sample = affinity_df.query('@min_trp <= TRP_кампании <= @max_trp and Рекламодатель != @adv')

    score = index[['Кампания', 'Рекламодатель', 'Дата_старта', 'Дата_окончания', 'TRP_кампании', 'GRP_кампании', 'Affinity_кампании',
                  'Affinity_рынка', 'Индекс против рынка']]
    score['Средний индекс в пуле'] = data_sample['Индекс против рынка'].mean()
    score['Индекс аффинитивности сплита'] = score['Индекс против рынка']/score['Средний индекс в пуле']*100
    score['Количество кампаний в пуле'] = len(data_sample)
    score['Процент разброса TRP'] = perc

    score = score.to_frame().T
    index = index.to_frame().T
    campaigns = data_sample[['Кампания', 'Дата_старта', 'Дата_окончания', 'ЦА']]

    # добавляем кампанию в наш пул
    
    if not ((index['Кампания'][0] in affinity_df['Кампания'].values)
            and (index['Дата_старта'][0] in affinity_df['Дата_старта'].values) and (index['Дата_окончания'][0] in affinity_df['Дата_окончания'].values)):
        new_data = pd.concat([affinity_df, index])
        new_data.to_excel('O:\\BMP Media Audit\\Media Research\\Проекты\\Оценка планирования\\Скрипты\\2. Оценка аффинити\\Пул кампаний с аффинити.xlsx',
                          index=False)

    # Сохраняем файл

    name = camp.replace('//', ' ')
    name = name.replace(':', '')
    with pd.ExcelWriter(save_path+str(name)+' Аффинити.xlsx', engine='openpyxl') as writer:
        score.to_excel(writer, sheet_name='Оценка аффинити', index=False)
        split.to_excel(writer, sheet_name='Эффективность сплита', index=False)
        affinity.to_excel(writer, sheet_name='Аффинитивность сплита', index=False)
        work.to_excel(writer, sheet_name='Расчёты', index=False)
        campaigns.to_excel(writer, sheet_name='Список анализируемых кампаний', index=False)


    return 'Расчёты готовы'

# окупаемость суперфикса
def sfix(row):
    camp = row['Кампания']
    trgt = row['ЦА']
    temp_df = temp_dataframe(row, camp)
    temp_affinity = affinity_camp(temp_df, trgt)
    aff_market = affinity_market(row)
    trp_market_prime, trp_market_op = prime_time(row)
    affinity_sfix = aff_market.merge(channels, on=['Ролик распространение', 'Телекомпания'])
    affinity_sfix['Ср. канальный аффинити'] = affinity_sfix['TRP_рынка']/affinity_sfix['GRP_рынка']
    affinity_sfix = affinity_sfix[['Телеканал', 'Номер', 'Ср. Sfix', 'Ср. канальный аффинити']]

    # праймтайм кампании
    camp_prime = temp_df.pivot_table(index='Телеканал', columns='Prime', values='Big TVR '+trgt, aggfunc=['count', 'sum']).reset_index()
    camp_prime.columns = ['Телеканал', 'Количество OP', 'Количество PT', 'Сумма TVR OP', 'Сумма TVR PT']
    camp_prime['Средний TVR OP'] = camp_prime['Сумма TVR OP']/camp_prime['Количество OP']
    camp_prime['Средний TVR PT'] = camp_prime['Сумма TVR PT']/camp_prime['Количество PT']
    camp_prime = camp_prime.fillna(0)

    # суперфикс из пба
    pba_temp = (pba.query('Кампания == @camp').groupby(by='Телеканал')
                [['План GRP"20 / мин ВСЕГО', 'План GRP"20 / мин SFix PT', 'План GRP"20 / мин SFix OP']].sum().reset_index()
        )
    pba_temp['Sfix'] = (pba_temp['План GRP"20 / мин SFix PT']+pba_temp['План GRP"20 / мин SFix OP'])/pba_temp['План GRP"20 / мин ВСЕГО']
    sfix = pba_temp[['Телеканал', 'Sfix']]

    # Считаем индекс аффинити
    index_affinity = temp_affinity.merge(affinity_sfix, on='Телеканал')
    index_affinity = sfix.merge(index_affinity, on='Телеканал')
    index_affinity = index_affinity.rename(columns={'Аффинити_кампании':'Фактический аффинити'})
    index_affinity['Индекс аффинити'] = index_affinity['Фактический аффинити']/index_affinity['Ср. канальный аффинити']
    index_affinity['Порог'] = (1+(index_affinity['Sfix']-index_affinity['Ср. Sfix'])*0.15)*100
    index_affinity['Индекс Sfix'] = index_affinity['Индекс аффинити']/index_affinity['Порог']*100
    index_affinity['GRP SOV'] = (index_affinity['GRP_кампании']/index_affinity['GRP_кампании'].sum())
    index_affinity = index_affinity.sort_values(by='Номер')
    index_affinity = index_affinity[['Телеканал', 'Sfix', 'Ср. канальный аффинити', 'GRP_кампании', 'GRP SOV', 'TRP_кампании',
                                     'Фактический аффинити', 'Индекс аффинити', 'Ср. Sfix', 'Порог', 'Индекс Sfix']]
    index_affinity_total = pd.DataFrame(columns=index_affinity.columns, index=[0])
    index_affinity_total['Телеканал'] = 'ИТОГО'
    index_affinity_total['Sfix'] = (index_affinity['Sfix']*index_affinity['GRP SOV']).sum()
    index_affinity_total['Ср. канальный аффинити'] = (index_affinity['Ср. канальный аффинити']*index_affinity['GRP SOV']).sum()
    index_affinity_total['GRP_кампании'] = index_affinity['GRP_кампании'].sum()
    index_affinity_total['GRP SOV'] = index_affinity['GRP SOV'].sum()
    index_affinity_total['TRP_кампании'] = index_affinity['TRP_кампании'].sum()
    index_affinity_total['Фактический аффинити'] = (index_affinity['Фактический аффинити']*index_affinity['GRP SOV']).sum()
    index_affinity_total['Индекс аффинити'] = index_affinity_total['Фактический аффинити']/index_affinity_total['Ср. канальный аффинити']
    index_affinity_total['Ср. Sfix'] = (index_affinity['Ср. Sfix']*index_affinity['GRP SOV']).sum()
    index_affinity_total['Порог'] = (1+(index_affinity_total['Sfix']-index_affinity_total['Ср. Sfix'])*0.15)*100
    index_affinity_total['Индекс Sfix'] = index_affinity_total['Индекс аффинити']/index_affinity_total['Порог']*100
    index_affinity = pd.concat([index_affinity, index_affinity_total])

    # Считаем индекс РТ и ОР
    index_prime_time = camp_prime.merge(trp_market_prime, on='Телеканал')
    index_prime_time = index_prime_time.merge(trp_market_op, on=['Телеканал', 'Номер'])
    index_prime_time['Индекс PT'] = index_prime_time['Средний TVR PT']/index_prime_time['TRP рынок PT']*100
    index_prime_time['Индекс OP'] = index_prime_time['Средний TVR OP']/index_prime_time['TRP рынок OP']*100
    index_prime_time = index_prime_time.sort_values(by='Номер')
    index_prime_time = index_prime_time[['Телеканал', 'TRP рынок PT', 'Средний TVR PT', 'Индекс PT', 'Количество PT', 'Сумма TVR PT',
                                         'TRP рынок OP', 'Средний TVR OP', 'Индекс OP', 'Количество OP', 'Сумма TVR OP']]
    index_prime_time_total = pd.DataFrame(columns=index_prime_time.columns, index=[0])
    index_prime_time_total['Телеканал'] = 'ИТОГО'
    index_prime_time_total['Количество OP'] = index_prime_time['Количество OP'].sum()
    index_prime_time_total['Количество PT'] = index_prime_time['Количество PT'].sum()
    index_prime_time_total['Сумма TVR OP'] = index_prime_time['Сумма TVR OP'].sum()
    index_prime_time_total['Сумма TVR PT'] = index_prime_time['Сумма TVR PT'].sum()
    index_prime_time_total['Средний TVR OP'] = ((index_prime_time['Средний TVR OP']*index_prime_time['Количество OP']).sum())/index_prime_time_total['Количество OP']
    index_prime_time_total['Средний TVR PT'] = ((index_prime_time['Средний TVR PT']*index_prime_time['Количество PT']).sum())/index_prime_time_total['Количество PT']
    index_prime_time_total['TRP рынок PT'] = ((index_prime_time['TRP рынок PT']*index_prime_time['Количество PT']).sum())/index_prime_time_total['Количество PT']
    index_prime_time_total['TRP рынок OP'] = ((index_prime_time['TRP рынок OP']*index_prime_time['Количество OP']).sum())/index_prime_time_total['Количество OP']
    index_prime_time_total['Индекс PT'] = index_prime_time_total['Средний TVR PT']/index_prime_time_total['TRP рынок PT']*100
    index_prime_time_total['Индекс OP'] = index_prime_time_total['Средний TVR OP']/index_prime_time_total['TRP рынок OP']*100
    index_prime_time = pd.concat([index_prime_time, index_prime_time_total])
    
    index_final = index_affinity.merge(index_prime_time, on='Телеканал')
    index_final = index_final[['Телеканал', 'Sfix', 'Порог', 'Индекс аффинити', 'Индекс PT', 'Индекс OP']]
    index_final['Индекс эффективности постановки'] = index_final['Индекс аффинити']*0.7+index_final['Индекс PT']*0.2+index_final['Индекс OP']*0.1
    index_final['Индекс окупаемости SuperFix'] = index_final['Индекс эффективности постановки']/index_final['Порог']*100
        
    name = camp.replace('//', ' ')
    name = name.replace(':', ' ')
    with pd.ExcelWriter(save_path+str(name)+' Окупаемость SuperFix.xlsx', engine='openpyxl') as writer:
        index_final.to_excel(writer, sheet_name='Индекс окупаемости SuperFix', index=False)
        index_affinity.to_excel(writer, sheet_name='Индекс аффинити', index=False)
        index_prime_time.to_excel(writer, sheet_name='Индекс prime off prime', index=False)

    return 'Расчёты готовы'

# доля ночи
def night(row):
    camp = row['Кампания']
    trgt = row['ЦА']
    temp_df = temp_dataframe(row, camp)
    temp_affinity = affinity_camp(temp_df, trgt)
    total_tvr = temp_affinity.merge(channels, on='Телеканал')
    total_tvr = total_tvr[['Телеканал', 'Номер', 'TRP_кампании']]

    night = (temp_df.query('`День / Ночь` == "Ночь"').pivot_table(index='Телеканал', values='Big TVR '+trgt, aggfunc='sum').reset_index())

    if night.empty:
        night_part = pd.DataFrame()
        night_part['Телеканал'] = temp_df['Телеканал'].unique()
        night_part['Ночной TVR'] = 0
    else:
        night.columns = ['Телеканал', 'Ночной TVR']
        night_part = night

    total_tvr = total_tvr.merge(night_part, on='Телеканал', how='left')
    total_tvr['Доля ночи'] = total_tvr['Ночной TVR']/total_tvr['TRP_кампании']
    total_tvr = total_tvr.fillna(0)

    name = camp.replace('//', ' ')
    name = name.replace(':', ' ')
    total_tvr.to_excel(save_path+str(name)+' Доля ночных выходов.xlsx', index=False)

    return 'Расчёты готовы'

# доля двойных выходов
def double_spot(row):
    camp = row['Кампания']
    trgt = row['ЦА']
    temp_df = temp_dataframe(row, camp)
    temp_affinity = affinity_camp(temp_df, trgt)
    total_tvr = temp_affinity.merge(channels, on='Телеканал')
    total_tvr = total_tvr[['Телеканал', 'Номер', 'TRP_кампании']]
    temp_df['Время'] = pd.to_datetime(temp_df['Дата'].astype(str) + ' ' + temp_df['Время начала (0-24)'].astype(str))
    temp_df.loc[temp_df['Час начала'] <= 4, 'Время'] += pd.DateOffset(days=1)

    double = (temp_df[['Телеканал', 'Блок ID выхода', 'Время', 'Big TVR '+trgt]].sort_values(by=['Блок ID выхода', 'Время']).reset_index(drop=True))
    double['number'] = double.groupby(['Блок ID выхода']).cumcount()+1
    double = double.query('number > 1').groupby('Телеканал')['Big TVR '+trgt].sum().reset_index()

    if double.empty:
        double_spot = pd.DataFrame()
        double_spot['Телеканал'] = temp_df['Телеканал'].unique()
        double_spot['Двойной TVR'] = 0
    else:
        double.columns = ['Телеканал', 'Двойной TVR']
        double_spot = double

    total_tvr = total_tvr.merge(double_spot, on='Телеканал', how='left')
    total_tvr['Доля двойных'] = total_tvr['Двойной TVR']/total_tvr['TRP_кампании']
    total_tvr = total_tvr.fillna(0)

    name = camp.replace('//', ' ')
    name = name.replace(':', ' ')
    total_tvr.to_excel(save_path+str(name)+' Доля двойных выходов.xlsx', index=False)

    return 'Расчёты готовы'

# доля премиум позиционирования
def premium(row):
    camp = row['Кампания']
    trgt = row['ЦА']
    temp_df = temp_dataframe(row, camp)
    df_premium = premium_market(row)
    
    df_premium.columns = ['Ролик распространение', 'Телекомпания', 'Ролик позиционирование', 'TRP рынка']
    df_premium['Телекомпания'] = df_premium['Телекомпания'].apply(get_cleared_channel)
    df_premium = df_premium.merge(channels, on=['Ролик распространение', 'Телекомпания'])
    df_premium['Позиционирование'] = df_premium['Ролик позиционирование'].apply(lambda x: 'Средний' if x == 'Средний' else 'Премиальное')
    df_premium = df_premium.pivot_table(index=['Телеканал', 'Номер'], columns = 'Позиционирование', values='TRP рынка', aggfunc='sum').reset_index()
    df_premium['TRP рынка'] = df_premium['Средний']+df_premium['Премиальное']
    df_premium['Доля премиум рынка'] = df_premium['Премиальное']/df_premium['TRP рынка']

    part_premium = temp_df.pivot_table(index='Телеканал', columns = 'Позиционирование', values='Big TVR '+trgt, aggfunc='sum').reset_index()
    part_premium['TRP кампании'] = part_premium['Средний']+part_premium['Премиальное']
    part_premium['Доля премиум кампании'] = part_premium['Премиальное']/part_premium['TRP кампании']
    
    premium = part_premium.merge(df_premium, on='Телеканал')
    premium['Индекс'] = premium['Доля премиум кампании']/premium['Доля премиум рынка']*100
    premium = premium.rename(columns= {'Премиальное_x': 'Премиальный TRP кампании', 'Премиальное_y': 'Премиальный TRP рынка'})
    premium = premium[['Телеканал', 'Номер', 'Премиальный TRP кампании', 'TRP кампании', 'Премиальный TRP рынка', 'TRP рынка',
                       'Доля премиум кампании', 'Доля премиум рынка', 'Индекс']]
    
    name = camp.replace('//', ' ')
    name = name.replace(':', ' ')
    premium.to_excel(save_path+str(name)+' Позиционирование.xlsx', index=False)

    return 'Расчёты готовы'


def print_menu():
    print('Добро пожаловать в программу!\n')
    print('Выберите скрипт для запуска:\n')

def print_choice():
    print()
    print('1. Оценка аффинити\n')
    print('2. Окупаемость суперфикса\n')
    print('3. Доля ночных выходов\n')
    print('4. Доля двойных выходов\n')
    print('5. Позиционирование\n\n')

def print_instructions():
    print('\nВведите номер скрипта, который вы хотите запустить.\n')
    print('Для выхода из программы введите 0.\n')

print_menu()
print_choice()
print_instructions()

while True:
    print('\nВведите номер скрипта (или несколько номеров без пробелов): ')
    choice = input()

    for i in choice:
        if int(i) == 1:
            data.apply(score_affinity, axis=1)  
        elif int(i) == 2:
            data.apply(sfix, axis=1)
        elif int(i) == 3:
            data.apply(night, axis=1)
        elif int(i) == 4:
            data.apply(double_spot, axis=1)
        elif int(i) == 5:
            data.apply(premium, axis=1)
        elif int(i) == 0:
            break
    else:
        print('\nНеверный ввод. Пожалуйста, выберите номер из меню.\n')
        print_choice()
