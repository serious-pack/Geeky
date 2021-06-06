#!/usr/bin/env python
# coding: utf-8

# ## Практические задания

# 1. Развернуть у себя на компьютере/виртуальной машине/хостинге MongoDB и реализовать функцию, записывающую собранные вакансии в созданную БД.
# 2. Написать функцию, которая производит поиск и выводит на экран вакансии с заработной платой больше введённой суммы.
# 3. Написать функцию, которая будет добавлять в вашу базу данных только новые вакансии с сайта.

# **Подключение библиотек и скриптов**
import requests
import json
import numpy as np
from bs4 import BeautifulSoup
import lxml
import pandas as pd


# **Вспомогательные переменные**
URL_HH = 'https://hh.ru'
URL_SJ = 'https://www.superjob.ru'
N_PAGES = 3

# **Вспомогательные процедуры и функции**

def get_vacancy_HH(vacancy_name, pages = N_PAGES):
    
    GET_HH_REQUEST = URL_HH + '/search/vacancy?clusters=true&enable_snippets=true&salary=&st=searchVacancy&text=' + vacancy_name
    HEADER = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}
    
    data_list = []
    
    next_link = GET_HH_REQUEST
    
    i = 1
    
    while next_link != '' and i <= pages:
        
        i += 1
        
        data = requests.get(next_link, headers = HEADER)

        if data.ok:
            html = BeautifulSoup(data.text, 'lxml')
            
            tags = html.find_all('div', attrs={'class': 'vacancy-serp-item'})

            for tag in tags:

                name        = tag.find('a', attrs = {'data-qa': 'vacancy-serp__vacancy-title'}).text
                href        = tag.find('a', attrs = {'data-qa': 'vacancy-serp__vacancy-title'})['href']
                salary      = tag.find('span', attrs = {'data-qa': 'vacancy-serp__vacancy-compensation'})
                salary_from = '0'
                salary_to   = '0'

                if salary:

                    salary_list = salary.text.replace('\u202f', '').split(' ')

                    if 'от' in salary_list:
                        salary_from = salary_list[salary_list.index('от') + 1]
                    if 'до' in salary_list:
                        salary_to = salary_list[salary_list.index('до') + 1]

                    else:
                        if '–' in salary_list:
                            salary_to   = salary_list[salary_list.index('–') + 1]
                            salary_from = salary_list[0]

                    if salary_from == '' and salary_to == '':
                        salary_to = salary_list[0]

                salary_from = int(salary_from)
                salary_to   = int(salary_to)

                company = tag.find('div', attrs = {'class', 'vacancy-serp-item__meta-info-company'}).text.replace('\xa0', '')

                data_list.append(['HH.ru', name, href, company, salary_from, salary_to])
                
        #найдем следующую ссылку
        tag = html.find('a', attrs={'data-qa': 'pager-next'})
        
        next_link = ''
        
        if tag:
            next_link = URL_HH + tag['href']
            
            
        
    df = pd.DataFrame(data_list, columns = ['Resource', 'Name', 'link', 'Company', 'Salary (from)', 'Salary (to)'])
        
    return df

def get_vacancy_SJ(vacancy_name,  pages = N_PAGES):
    
    GET_SJ_REQUEST = URL_SJ + '/vacancy/search/?keywords=' + vacancy_name
    HEADER = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}
    
    data_list = []
    
    next_link = GET_SJ_REQUEST
    
    i = 1
    
    while next_link != '' and i <= pages:
        
        i += 1
        
        data = requests.get(next_link, headers = HEADER)

        if data.ok:
            html = BeautifulSoup(data.text, 'lxml')
            
            tags = html.find_all('div', attrs={'class': 'iJCa5 f-test-vacancy-item _1fma_ _2nteL'})

            for tag in tags:

                name_duty   = tag.find('div', attrs = {'class': '_1h3Zg _2rfUm _2hCDz _21a7u'})
                name        = name_duty.find('a').text
                
                href        = URL_SJ + tag.find('a')['href']
                
                salary      = tag.find('span', attrs = {'class': '_1h3Zg _2Wp8I _2rfUm _2hCDz _2ZsgW'})
                                
                salary_from = '0'
                salary_to   = '0'

                if salary:

                    salary_list = salary.text.replace('\u202f', '').replace('0\xa00', '00').replace('\xa0', ' ').split(' ')
                    
                    if 'от' in salary_list:
                        salary_from = salary_list[salary_list.index('от') + 1]
                    if 'до' in salary_list:
                        salary_to = salary_list[salary_list.index('до') + 1]

                    else:
                        if '—' in salary_list:
                            salary_to   = salary_list[salary_list.index('—') + 1]
                            salary_from = salary_list[0]##

                    if salary_from == '' and salary_to == '' and salary_list[0] != 'По':
                        salary_to = salary_list[0]

                salary_from = int(salary_from)
                salary_to   = int(salary_to)
                company_duty = tag.find('span', attrs = {'class', '_1h3Zg _3Fsn4 f-test-text-vacancy-item-company-name e5P5i _2hCDz _2ZsgW _2SvHc'})
                
                if company_duty:
                    company = company_duty.find('a').text
                else:
                    company = ''
                
                data_list.append(['SJ.ru', name, href, company, salary_from, salary_to])
                
        #найдем следующую ссылку
        tag = html.find('a', attrs={'rel': 'next'})
        
        next_link = ''
        
        if tag:
            next_link = URL_SJ + tag['href']
            
            
        
    df = pd.DataFrame(data_list, columns = ['Resource', 'Name', 'link', 'Company', 'Salary (from)', 'Salary (to)'])
        
    return df

df = pd.concat([get_vacancy_SJ('уполномоченный', 2), get_vacancy_SJ('уполномоченный', 2)])
df.info()

def opening_dbMongo():
    from pymongo import MongoClient
    
    client = MongoClient('mongodb://85.246.89.240:7000')
    #client.GB_HH.authenticate('*********', '***********', mechanism = 'SCRAM-SHA-1', source='source_database')
    db = client['GB_HH']
    collection = db.test_collection
    
    print(collection)
    
    return db

def puttoMongo(dbm, dfm):
    
    collection = dbm.test_collection
    
    dfm['Ind'] = np.arange(len(dfm))
    listofd = dfm.set_index('Ind').T.to_dict().values()
    
    collection.insert_many(listofd)
    
    return collection

def findMongo(dbm, salary):
    
    collection = dbm.test_collection
    
    return collection.find({ 'Salary (from)': {"$gte": salary} });

def puttoMongo_only_new(dbm, dfm):
    
    collection = dbm.test_collection
    
    dfm['Ind'] = np.arange(len(dfm))
    listofd = dfm.set_index('Ind').T.to_dict().values()
    
    for record in listofd:
        rec_rev = collection.find_one({'link': record['link']})
        
        #Проверим что изменилась или что это новая запись
        if not rec_rev: #Новая запись
            collection.insert_one(record)
            print('Новая запись:')
            print(record)
        else: #возможно изменилась
            del rec_rev['_id']
            if '_id' in record: del record['_id']
                        
            if rec_rev != record:
       
                print('Измененная запись:')
                print(rec_rev)
                
                collection.update_one({'link' : record['link']}, {"$set":record})

def clear_dbMongo(dbm, filter_):
    
    collection = dbm.test_collection
    collection.delete_many(filter_)


db = opening_dbMongo()
clear_dbMongo(db, {'Resource':'HH.ru'})
clear_dbMongo(db, {'Resource':'SJ.ru'})

# ## Запись в Mongo
puttoMongo(db, df)

# ## Фильтр в Mongo
coll = findMongo(db, 100000)

for c in coll:
    print(c)

# ## Запись только новых в Mongo

df_upd = pd.concat([get_vacancy_SJ('программист', 2), get_vacancy_SJ('программист', 2)])
puttoMongo_only_new(db, df_upd)

