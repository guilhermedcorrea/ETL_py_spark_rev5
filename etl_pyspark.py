
from pyspark.sql import SparkSession
from pyspark.sql import Row
from dotenv import load_dotenv
from urllib import parse
from dotenv import load_dotenv
from os import path
import os


load_dotenv()

user = os.getenv('user')
password = os.getenv('password')
tabela = os.getenv('table')
url = os.getenv('URI_BI')


header='True',


def reader_csv(spark: SparkSession) -> None:
    df = spark.read.option("inferSchema",True) \
                 .options(header='True', inferSchema='True', delimiter=';') \
                .csv(r"C:\estimadoresteste\ETL_py_spark_rev5\controllers\exemplo_loja.csv")
    
    NEW = df.select("Nomedoproduto","IDdoproduto","Categoria").filter("Categoria == 'Tecnologia'")
    print(df.show(10))

    NEW.createOrReplaceTempView("ProdutosView")

    spark.sql("CREATE TABLE IF NOT EXISTS produtos (Nomedoproduto String, IDdoproduto Int, Categoria String)")

    spark.sql("INSERT INTO TABLE produtos  SELECT * FROM ProdutosView")


def jdbc_dataset_example(spark: SparkSession) -> None:

    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("dbtable", tabela) \
        .option("user", user) \
        .option("password", password)\
        .load()
    

    jdbcDF.write.mode('overwrite') \
         .saveAsTable(tabela)
    
   

    df = spark.read.table(tabela)
    df.show()
    

def reader_table(spark: SparkSession) -> None:
    spark.sql("select  * FROM produtos")


