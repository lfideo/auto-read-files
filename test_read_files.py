import read_files as rf # импортирую модуль для выгрузки таблицы из базы данных
import pandas as pd
import numpy as np

# читаю файл из базы данных 
df = rf.ReadSQL.read_sql_file(
    'mssql+pymssql://database_address', # адрес базы данных
    'table_name' # имя таблицы - тестовая выгрзука из яндекс метрики
)

# оставляю нужные иточники и удаляю ненужный старый столбец
df = df.loc[df.source.isin(['ad', 'social', 'internal', 'messenger'])]
df.drop(columns='source', inplace=True)

# узнаю список уникальных источников для последующего парсинга
print(df.detail.unique())

# создаю столбец с источниками
# здесь происходит поиск названий источников в столбцах utm_source и источники трафика (детально)
df = df.assign(
    source = np.where(
            df.detail.str.contains(r'яндекс|yandex', case=False, regex=True), 'yandex',
        # np.where(df.utm_source.str.contains(r'yandex', case=False, regex=True), 'yandex', 
        np.where(df.detail.str.contains(r'google ads|google', case=False, regex=True), 'google ads',
        np.where(df.detail.str.contains(r'facebook|instagram|fb', case=False, regex=True), 'facebook / instagram',
        np.where(df.utm_source.str.contains(r'instagram|inst', case=False, regex=True), 'facebook / instagram',
        np.where(df.detail.str.contains(r'Таргет@Mail.ru', case=False, regex=True), 'mytarget',
        np.where(df.utm_source.str.contains(r'mytarget', case=False, regex=True), 'mytarget',
        np.where(df.detail.str.contains(r'vk|vkontakte|ВКонтакте', case=False, regex=True), 'vkontakte',
        np.where(df.utm_source.str.contains(r'vk|vkontakte|ВКонтакте', case=False, regex=True), 'vkontakte',
        np.where(df.detail.str.contains(r'criteo', case=False, regex=True), 'criteo',
        np.where(df.detail.str.contains(r'rtbhouse|rtb', case=False, regex=True), 'rtbhouse',
        np.where(df.detail.str.contains(r'youtube', case=False, regex=True), 'youtube',
        np.where(df.detail.str.contains(r'getintent', case=False, regex=True), 'getintent',
        np.where(df.detail.str.contains(r'otm', case=False, regex=True), 'otm',
        np.where(df.utm_campaign.str.contains(r'tw_november_sadi_|tw_october_sad', case=False, regex=True), 'posevy',                 
        np.where(df.detail.str.contains(r'telegram', case=False, regex=True), 'telegram',        
        np.where(df.utm_source.str.contains(r'segmento', case=False, regex=True), 'segmento',
        np.where(df.utm_source.str.contains(r'nt_technology', case=False, regex=True), 'nt_technology',
        np.where(df.utm_source.str.contains(r'orm', case=False, regex=True), 'orm',
        np.where(df.utm_source.str.contains(r'slickjump', case=False, regex=True), 'slickjump', 'Другое')
    ))))))))))))))))))
)

# делаю группировку по источнику и считаю суммы по каждому значению
gr = df.groupby(['source'])[[
    'users', 'new_users', 'orders', 'orders_users'
]].agg({
    'users': np.sum, 'new_users': np.sum,
    'orders': np.sum, 'orders_users': np.sum
})

# создаю столбец с долей новых посетителей сайта
gr['% new users'] = gr.new_users / gr.users

# сохраняю файл в csv
gr.to_csv('new_users.csv')