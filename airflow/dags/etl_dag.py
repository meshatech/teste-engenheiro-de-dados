from datetime import datetime,date, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType 
from pyspark.sql import functions as f
import findspark

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
}

dag = DAG('etl_data', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )

findspark.init()

spark = SparkSession.builder.master('local[*]')

pathRow = "/workspace/teste-engenheiro-de-dados/dados/row/DADOS/MICRODADOS_ENEM_2020.csv"
pathParquetProcessing = "/workspace/teste-engenheiro-de-dados/dados/processing"


def extracao_dados():

    #Extração dos dados

    enem = spark.read.csv(pathRow, sep=";", header=True)

    #Seleção das Colunas

    enemSelecao = enem.select("NU_INSCRICAO", "TP_SEXO", "TP_COR_RACA", "CO_MUNICIPIO_ESC", "NO_MUNICIPIO_ESC", "SG_UF_ESC", "TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT", "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO", "TP_ST_CONCLUSAO")

    #Correção dos tipos dos dados

    enemSelecaoTipos = enemSelecao\
    .withColumn(
    "TP_COR_RACA",
    enemSelecao["TP_COR_RACA"].cast(IntegerType())
    )\
    .withColumn(
    "CO_MUNICIPIO_ESC",
    enemSelecao["CO_MUNICIPIO_ESC"].cast(IntegerType())
    )\
    .withColumn(
    "TP_PRESENCA_CN",
    enemSelecao["TP_PRESENCA_CN"].cast(IntegerType())
    )\
    .withColumn(
    "TP_PRESENCA_CH",
    enemSelecao["TP_PRESENCA_CH"].cast(IntegerType())
    )\
    .withColumn(
    "TP_PRESENCA_LC",
    enemSelecao["TP_PRESENCA_LC"].cast(IntegerType())
    )\
    .withColumn(
    "TP_PRESENCA_MT",
    enemSelecao["TP_PRESENCA_MT"].cast(IntegerType())
    )\
    .withColumn(
    "NU_NOTA_CN",
    enemSelecao["NU_NOTA_CN"].cast(DoubleType())
    )\
    .withColumn(
    "NU_NOTA_CH",
    enemSelecao["NU_NOTA_CH"].cast(DoubleType())
    )\
    .withColumn(
    "NU_NOTA_LC",
    enemSelecao["NU_NOTA_LC"].cast(DoubleType())
    )\
    .withColumn(
    "NU_NOTA_MT",
    enemSelecao["NU_NOTA_MT"].cast(DoubleType())
    )\
    .withColumn(
    "NU_NOTA_REDACAO",
    enemSelecao["NU_NOTA_REDACAO"].cast(IntegerType())
    )\
    .withColumn(
    "TP_ST_CONCLUSAO",
    enemSelecao["TP_ST_CONCLUSAO"].cast(IntegerType())
    )

    novosNomes = {'NU_INSCRICAO':'NumInscricao', 'TP_SEXO':'Sexo', 'TP_COR_RACA':'Etnia', 'CO_MUNICIPIO_ESC':'CodMunicipioEsc', 'NO_MUNICIPIO_ESC':'NomMunicipioEsc', 'SG_UF_ESC':'EstadoEsc', 'TP_PRESENCA_CN':'PresencaCN', 'TP_PRESENCA_CH':'PresencaCH', 'TP_PRESENCA_LC':'PresencaLC', 'TP_PRESENCA_MT':'PresencaMT', 'NU_NOTA_CN':'NotaCN', 'NU_NOTA_CH':'NotaCH', 'NU_NOTA_LC':'NotaLC', 'NU_NOTA_MT':'NotaMT', 'NU_NOTA_REDACAO':'NotaRedacao', 'TP_ST_CONCLUSAO':'SituacaoConclusao'}

    for key, value in novosNomes.items():
        enemSelecaoTipos = enemSelecaoTipos.withColumnRenamed(key, value)

    #Transformação para o formato Parquet - Processing Zone

    pathParquet = "/workspace/teste-engenheiro-de-dados/dados/processing"

    enemSelecaoTipos.write.parquet(
    pathParquet,
    mode = "overwrite"
    )

 def curated_zone_pergunta_01:

    # 1 - Qual a escola com a maior média de notas?

    enemParquet = spark.read.parquet(
    pathParquetProcessing
    )

    enemParquet.createOrReplaceTempView("enemView")

    escolaMaiorMedia = spark.sql("""
        SELECT 
            CodMunicipioEsc, NomMunicipioEsc, 
            EstadoEsc, ROUND(AVG(MediaNotas), 2) AS MediaNotas
        FROM
            (SELECT
                NumInscricao, CodMunicipioEsc, NomMunicipioEsc, EstadoEsc, 
                ROUND((NotaCN+NotaCH+NotaLC+NotaMT+NotaRedacao)/5, 2) AS MediaNotas
            FROM
                enemView
            WHERE 
                    SituacaoConclusao == 2 
                AND 
                    CodMunicipioEsc IS NOT NULL 
                AND 
                    NotaMT IS NOT NULL
                AND
                    NotaCH IS NOT NULL
                AND
                    NotaCN IS NOT NULL
                AND
                    NotaLC IS NOT NULL
                AND 
                    NotaRedacao IS NOT NULL
            )
        GROUP BY CodMunicipioEsc, NomMunicipioEsc, EstadoEsc  
        ORDER BY MediaNotas DESC
        """)

        escolaMaiorMedia.limit(10).show()

extracao_dados = PythonOperator(
  task_id='extracao_dados',
  provide_context=True,
  python_callable=extracao_dados,
  dag=dag
)

curated_zone_pergunta_01 = PythonOperator(
  task_id='curated_zone_pergunta_01',
  provide_context=True,
  python_callable=curated_zone_pergunta_01,
  dag=dag
)

extracao_dados >> curated_zone_pergunta_01




    

