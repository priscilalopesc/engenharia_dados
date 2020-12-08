# Databricks notebook source
# DBTITLE 1,Inicialização
from pyspark.sql.functions import when, col, lit, concat, trim , expr
import os
from pyspark.sql.types import DoubleType

# COMMAND ------------

# DBTITLE 1,Monta o Data Lake
def mount_blob(account_name, account_key, container):
  mounts = [mount.mountPoint for mount in dbutils.fs.mounts()]
  
  if f'/mnt/{container}' not in mounts:
    blob_prefix = f'wasbs://{container}@{account_name}.blob.core.windows.net/'
    dbutils.fs.mount(
      source = blob_prefix,
      mount_point = f'/mnt/{container}',
      extra_configs = {
        f'fs.azure.account.key.{account_name}.blob.core.windows.net': account_key
      }
    )

# dbutils.fs.unmount('/mnt/dados')
mount_blob(
  account_name='cantudatalakedev',
  account_key='AYMJd3bnC/UipbQUm7/he2YfreK5VFoTaPyvC/SwLT0ZGbsDmjpaYt3jlSq1lgMkqC3jvyw1kToMVF+Z6xSMdA==',
  container='dados'
)

# COMMAND ----------

# DBTITLE 1,Função que prepara a conexão com o SQL Server
def connect_to_sql_server(jdbcUsername, jdbcPassword, jdbcHostname=None, jdbcPort=1433, jdbcDatabase=None):
  jdbc_url = \
    f'jdbc:sqlserver://{jdbcHostname}:{jdbcPort};' \
    f'database={jdbcDatabase};' \
    'encrypt=true;' \
    'trustServerCertificate=false;' \
    'hostNameInCertificate=*.database.windows.net;' \
    'loginTimeout=60;'

  jdbc_properties = {
      "user": jdbcUsername,
      "password": jdbcPassword
  }
  
  return (jdbc_url, jdbc_properties)

# COMMAND ----------

def get_max_path_date_from_path(path):
  max_year = max([item.name for item in dbutils.fs.ls(path)])
  max_month = max([item.name for item in dbutils.fs.ls(os.path.join(path, max_year))])
  max_day = max([item.name for item in dbutils.fs.ls(os.path.join(path, max_year, max_month))])
  max_path_date = os.path.join(path, max_year, max_month, max_day)

  return max_path_date

# COMMAND ----------

# DBTITLE 1,Lê os arquivos de origem
df_sf1010 = spark.read.csv(get_max_path_date_from_path('/mnt/dados/datalake/raw/source/oracle/totvs131.SF1010'), sep=',', header=True)
df_sd1010 = spark.read.csv(get_max_path_date_from_path('/mnt/dados/datalake/raw/source/oracle/totvs131.SD1010'), sep=',', header=True)

# COMMAND ----------

# DBTITLE 1,Filtro de Campo do Extrator ERP_Itens_NFE
df_sd1010_emissao = df_sd1010 \
  .filter(col("D1_FILIAL").between('0800', '8099'))

# COMMAND ----------

# DBTITLE 1,Ajuste de String para Date
df_sf1010_Ajuste_Data = df_sf1010 \
  .withColumn("F1_DTDIGIT",expr("to_date(F1_DTDIGIT, 'yyyymmdd')"))

df_sd1010_Ajuste_Data = df_sd1010_emissao \
  .withColumn("D1_DTDIGIT",expr("to_date(D1_DTDIGIT, 'yyyymmdd')"))

# COMMAND ----------

# DBTITLE 1,Condição Extrator ERP_CABECALHO_NFE
df_sf1010_where = df_sf1010_Ajuste_Data \
  .filter(~col("F1_FILIAL").like("98%"))

# COMMAND ----------

# DBTITLE 1,Remoção de Espaços
df_sf1010_remove_space = df_sf1010_where \
  .withColumn("F1_FILIAL", trim(df_sf1010_where.F1_FILIAL)) \
  .withColumn("F1_DOC", trim(df_sf1010_where.F1_DOC)) \
  .withColumn("F1_SERIE", trim(df_sf1010_where.F1_SERIE)) \
  .withColumn("F1_FORNECE", trim(df_sf1010_where.F1_FORNECE)) \
  .withColumn("F1_LOJA", trim(df_sf1010_where.F1_LOJA))

df_sd1010_remove_space = df_sd1010_Ajuste_Data \
    .withColumn("D1_FILIAL", trim(df_sd1010_Ajuste_Data.D1_FILIAL)) \
    .withColumn("D1_SERIE", trim(df_sd1010_Ajuste_Data.D1_SERIE)) \
    .withColumn("D1_DOC", trim(df_sd1010_Ajuste_Data.D1_DOC)) \
    .withColumn("D1_FORNECE", trim(df_sd1010_Ajuste_Data.D1_FORNECE)) \
    .withColumn("D1_LOJA", trim(df_sd1010_Ajuste_Data.D1_LOJA)) \
    .withColumn("D1_NFORI", trim(df_sd1010_Ajuste_Data.D1_NFORI)) \
    .withColumn("D1_SERIORI", trim(df_sd1010_Ajuste_Data.D1_SERIORI)) \
    .withColumn("D1_FILORI", trim(df_sd1010_Ajuste_Data.D1_FILORI)) \
    .withColumn("D1_COD", trim(df_sd1010_Ajuste_Data.D1_COD)) \
    .withColumn("D1_TIPO", trim(df_sd1010_Ajuste_Data.D1_TIPO)) \
    .withColumn("D1_CLVL", trim(df_sd1010_Ajuste_Data.D1_CLVL))

# COMMAND ----------

# DBTITLE 1,Seleciona as duas primeiras posições do campo 
df_sf1010_filial = df_sf1010_remove_space \
  .withColumn("F1_COD_EMPRESA_CTE", df_sf1010_remove_space.F1_FILIAL[0:2]
             )

df_sd1010_filial = df_sd1010_remove_space \
  .withColumn("D1_COD_EMPRESA_CTE", df_sd1010_remove_space.D1_FILIAL[0:2])\
  .withColumn("SEGMENTO_RAIZ_CTE", df_sd1010_remove_space.D1_CLVL[0:3])\
  .withColumn("SEGMENTO_MEDIO_CTE", df_sd1010_remove_space.D1_CLVL[0:6])

# COMMAND ----------

# DBTITLE 1,Concat
df_sd1010_concat = df_sd1010_filial \
  .withColumn('_CODIGO_TRANSP', concat('D1_FORNECE', 'D1_LOJA')
             )

df_sd1010_case = df_sd1010_concat \
 .withColumn('FILIAL NOTA', 
   when(col('D1_FILORI') == '', df_sd1010_concat.D1_FILIAL)
   .otherwise(df_sd1010_concat.D1_FILORI)       
            ) 

df_sd1010_float = df_sd1010_case \
  .withColumn("D1_TOTAL",
    df_sd1010_case.D1_TOTAL.cast(DoubleType())
             ) \
  .withColumn("D1_VALIPI",
    df_sd1010_case.D1_VALIPI.cast(DoubleType())
             ) \
  .withColumn("D1_ICMSRET",
    df_sd1010_case.D1_ICMSRET.cast(DoubleType())
             )

df_sd1010_sum = df_sd1010_float \
  .withColumn("VAL BRUTO CTE",
    when(col('D1_TIPO') == 'C', df_sd1010_float.D1_TOTAL + df_sd1010_float.D1_VALIPI)
    .otherwise(df_sd1010_float.D1_TOTAL + df_sd1010_float.D1_VALIPI + df_sd1010_float.D1_ICMSRET)
             )

# COMMAND ----------

# DBTITLE 1,Joins
df_sf1010_join = df_sf1010_filial.alias('sf1010') \
  .join(
    df_sd1010_sum.alias('sd1010'),
    (col('sf1010.F1_FILIAL') == col('sd1010.D1_FILIAL')) \
  & (col('sf1010.F1_COD_EMPRESA_CTE') == col('sd1010.D1_COD_EMPRESA_CTE')) \
  & (col('sf1010.F1_DOC') == col('sd1010.D1_DOC')) \
  & (col('sf1010.F1_SERIE') == col('sd1010.D1_SERIE')) \
  & (col('sf1010.F1_FORNECE') == col('sd1010.D1_FORNECE')) \
  & (col('sf1010.F1_LOJA') == col('sd1010.D1_LOJA')) \
  & (col('sf1010.F1_DTDIGIT') == col('sd1010.D1_DTDIGIT')),
    how='inner'
  ) 

# COMMAND ----------

# DBTITLE 1, Selects e renames
df_sf1010_select = df_sf1010_join \
  .select(
    col('F1_FILIAL').alias('FILIAL_CTE'),
    col('F1_COD_EMPRESA_CTE').alias('COD_EMPRESA_CTE'),
    col('F1_DOC').alias('NUMERO_CTE'),
    col('F1_SERIE').alias('SERIE_CTE'),
    col('F1_FORNECE').alias('CODIGO_TRANSP'),
    col('F1_LOJA').alias('LOJA_TRANSP'),
    col('F1_DTDIGIT').alias('DATA_DIGITACAO_CTE'),
    col('F1_ESPECIE').alias('ESPECIE_CTE'),
    col('F1_NATUREZ').alias('NATUREZA'),
    col('_CODIGO_TRANSP').alias('_CODIGO_TRANSP'),
    col('D1_NFORI').alias('NUM_NOTA'),
    col('D1_SERIORI').alias('SERIE_NOTA'),
    col('FILIAL NOTA').alias('FILIAL_NOTA'),
    col('D1_DTDIGIT').alias('D1_DTDIGIT_NFE'),
    col('D1_TES').alias('CODIGO_TES_CTE'),
    col('D1_COD').alias('CODIGO_PRODUTO'),
    col('D1_TIPO').alias('TIPO_CTE'),
    col('D1_CF').alias('CFOP_ENT'),
    col('SEGMENTO_RAIZ_CTE').alias('_SEGMENTO_RAIZ_CTE'),
    col('SEGMENTO_MEDIO_CTE').alias('_SEGMENTO_MEDIO_CTE'),
    col('D1_CLVL').alias('_SEGMENTO_CTE'),
    col('D1_CC').alias('CENTRO_CUSTO_ENT'),
    col('D1_CONTA').alias('CONTA_CONTABIL_CTE'),
    col('D1_PESO').alias('PESO_TOTAL_CTE'),
    col('VAL BRUTO CTE').alias('VAL_BRUTO_CTE'),
    col('D1_VALFRE').alias('VALOR_FRETE')
)

# COMMAND ----------

# DBTITLE 1,Salva os dados no SQL Server
print('Getting JDBC connection string...')
jdbc_url, jdbc_properties = connect_to_sql_server(
  jdbcUsername='admin-db',
  jdbcPassword='VuA0nRSevo',
  jdbcHostname='csrv01cantu.database.windows.net',
  jdbcPort=1433,
  jdbcDatabase='dbdatalakedev'
)

print('Writing to SQL Server...')
df_sf1010_select.write.jdbc(url=jdbc_url, table='Fato_Frete_Faturamento', mode='overwrite', properties=jdbc_properties)

print('Done!')
