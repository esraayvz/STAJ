#Bu kodda, excel'deki veri setimin sütunlarındaki veri tiplerini, SQL Server veri tiplerine dönüştürdüm ve doğru veri tipleriyle, SQL server da daha önce oluşturduğum database'in içinde 
#bir tablo oluşturdum. Sonra bu tabloya exceldeki verileri aktarmak için sqlalchemy kullanarak kendi sql server'ıma bağlandım ve böylece exceldeki verileri sql de oluşturduğum
#tabloya basmış oldum.
#ESRA AYVAZ

import pandas as pd
from sqlalchemy import create_engine #sql-python arası bağlantı
import urllib #url'yi güvenli hale getirir.

#excel dosyasındaki her sütunun veri tipini SQL Server veri tipine dönüştüren fonksiyon
def excel_to_sql_types(df):
    sql_types = {}
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            sql_types[col] = 'INT'
        elif pd.api.types.is_float_dtype(dtype):
            sql_types[col] = 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            sql_types[col] = 'BIT'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_types[col] = 'DATETIME'
        else:
            max_len = df[col].astype(str).map(len).max()
            if max_len <= 50:
                sql_types[col] = 'NVARCHAR(50)'
            elif max_len <= 255:
                sql_types[col] = 'NVARCHAR(255)'
            else:
                sql_types[col] = 'NVARCHAR(MAX)'
    return sql_types

#excel dosyasını okutuyorum.
excel_dosyasi = r"D:\DATASET.xlsx"
df = pd.read_excel(excel_dosyasi)

#veri tiplerini yazdırıyorum.
tipler = excel_to_sql_types(df)
print("Excel sütunları ve tahmini SQL Server veri tipleri:")
for col, tip in tipler.items():
    print(f"{col}: {tip}")

#SQL server'a bağlanmam gerekli.
server = 'DESKTOP-ID8KEUH\\TTSQL'
database = 'ETL_DB'

#sql'e bağlanmak için gereken bilgileri url formatına dönüştürüyorum ki sqlalchemy beni sql'e bağlayabilsin.
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"  # Windows Authentication kullanımı
    "TrustServerCertificate=yes;" # Sertifika doğrulamasını atla
)

#sqlalchemy'nin create engine fonksiyonu python ile sql arası bağlantıyı url kodlamasını kullanarak kurar.
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

#verileri oluşturduğum tabloya ekliyorum.
df.to_sql('ETL_DATA', con=engine, if_exists='append', index=False)

#verileri aktarabilirsem yani kod çalışırsa başarı mesajı yazacak.
print("Veri başarıyla aktarıldı!")

#ESRA AYVAZ 


