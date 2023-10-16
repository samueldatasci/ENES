import pandas as pd


parquetPath = dicParams['dataFolderParquet']

dfGeo = pd.read_parquet(parquetPath + 'dfGeo.parquet.gzip')  
dfSchools = pd.read_parquet(parquetPath + 'dfSchools.parquet.gzip')
dfSitFreq = pd.read_parquet(parquetPath + 'dfSitFreq.parquet.gzip')


