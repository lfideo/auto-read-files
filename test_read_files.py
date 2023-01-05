import read_files as rf # кастомный клас для чтения разных типов файлов

df = rf.ReadSQL.read_sql_file(
    'mssql+pymssql://mt_user:rbD3qHIqO2DZ0bdZ@mtools.digitaltwiga.ru:21433/reports', # адрес базы данных
    'yandex_metrika_sad_i_ogorod_monthconverion' # имя таблицы
)

print(df)