import pandas as pd
url = 'https://github.com/erkansirin78/datasets/raw/master/dirty_store_transactions.csv'
df = pd.read_csv(url)
print(df.head())

STORE_ID STORE_LOCATION PRODUCT_CATEGORY PRODUCT_ID  MRP      CP DISCOUNT  \
0   YR7220      New York(      Electronics   12254943  $31  $20.77    $1.86   
1   YR7220      New York+        Furniture  72619323C  $15   $9.75     $1.5   
2   YR7220      New York       Electronics  34161682B  $88  $62.48     $4.4   
3   YR7220      New York!          Kitchen   79411621  $91  $58.24    $3.64   
4   YR7220       New York          Fashion  39520263T  $85     $51    $2.55   

       SP        Date  
0  $29.14  2019-11-26  
1   $13.5  2019-11-26  
2   $83.6  2019-11-26  
3  $87.36  2019-11-26  
4  $82.45  2019-11-26  

df['STORE_ID'] = df['STORE_ID'].str.replace(r'[^\w\s]', '', regex=True)
df['STORE_LOCATION'] = df['STORE_LOCATION'].str.replace(r'[^\w\s]', '', regex=True)
df['PRODUCT_CATEGORY'] = df['PRODUCT_CATEGORY'].str.replace(r'[^\w\s]', '', regex=True)
print(df[['STORE_ID', 'STORE_LOCATION', 'PRODUCT_CATEGORY']].head())

 STORE_ID STORE_LOCATION PRODUCT_CATEGORY
0   YR7220       New York      Electronics
1   YR7220       New York        Furniture
2   YR7220      New York       Electronics
3   YR7220       New York          Kitchen
4   YR7220       New York          Fashion
df['MRP'] = df['MRP'].replace(r'[^\d.]', '', regex=True).astype(float)
df['CP'] = df['CP'].replace(r'[^\d.]', '', regex=True).astype(float)
df['DISCOUNT'] = df['DISCOUNT'].replace(r'[^\d.]', '', regex=True).astype(float)
df['SP'] = df['SP'].replace(r'[^\d.]', '', regex=True).astype(float)
print(df[['MRP', 'CP', 'DISCOUNT', 'SP']].head())

 MRP     CP  DISCOUNT     SP
0  31.0  20.77      1.86  29.14
1  15.0   9.75      1.50  13.50
2  88.0  62.48      4.40  83.60
3  91.0  58.24      3.64  87.36
4  85.0  51.00      2.55  82.45
df['PRODUCT_ID'] = pd.to_numeric(df['PRODUCT_ID'], errors='coerce')
df = df.dropna(subset=['PRODUCT_ID'])
df['PRODUCT_ID'] = df['PRODUCT_ID'].astype(int)
print(df[['PRODUCT_ID']].head())

PRODUCT_ID
0    12254943
3    79411621
5    93809204
8    77516479
9    47334289
df['Date_Casted'] = pd.to_datetime(df['Date'], errors='coerce')
print(df[['Date', 'Date_Casted']].head())

   Date Date_Casted
0  2019-11-26  2019-11-26
3  2019-11-26  2019-11-26
5  2019-11-26  2019-11-26
8  2019-11-26  2019-11-26
9  2019-11-26  2019-11-26

import pyarrow as pa
import pyarrow.orc as orc
table = pa.Table.from_pandas(df)
with pa.OSFile('clean_store_transactions.orc', 'wb') as sink:
    orc.write_table(table, sink)
import os
if os.path.exists('clean_store_transactions.orc'):
    print("Dosya başarıyla oluşturuldu.")
else:
    print("Dosya oluşturulamadı.")

Dosya başarıyla oluşturuldu.

with pa.OSFile('clean_store_transactions.orc', 'rb') as source:
    orc_table = orc.read_table(source)
df_orc = orc_table.to_pandas()
print(df_orc.head())

STORE_ID STORE_LOCATION PRODUCT_CATEGORY  PRODUCT_ID   MRP     CP  DISCOUNT  \
0   YR7220       New York      Electronics    12254943  31.0  20.77      1.86   
1   YR7220       New York          Kitchen    79411621  91.0  58.24      3.64   
2   YR7220       New York          Kitchen    93809204  37.0  24.05      0.74   
3   YR7220       New York          Kitchen    77516479  92.0  56.12      3.68   
4   YR7220       New York        Cosmetics    47334289  16.0  10.72      0.96   

      SP        Date Date_Casted  __index_level_0__  
0  29.14  2019-11-26  2019-11-26                  0  
1  87.36  2019-11-26  2019-11-26                  3  
2  36.26  2019-11-26  2019-11-26                  5  
3  88.32  2019-11-26  2019-11-26                  8  
4  15.04  2019-11-26  2019-11-26                  9  

