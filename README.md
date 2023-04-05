##Execução do projeto
<br>criar venv: py -3 -m venv venv<br/>
<br>ativar: venv\Scripts\activate<br/>
<br>pip install pyspark --upgrade<br/>
<br>pip install pyspark[sql]<br/>
<br>pip install PyHive<br/>
<br>pip install hdfs<br/>

<br>Após a instalação fazer o download do arquivo JDBC referente ao banco de dados. Ex o meu é o SQL server. Apos o download pegar e mover para dentro da pasta PySPARK no ambiente virtual, e tambem para dentro da pasra jars dentro da pyspark "C:\estimadoresteste\ETL_py_spark_rev5\venv\Lib\site-packages\pyspark"
e a jars "C:\estimadoresteste\ETL_py_spark_rev5\venv\Lib\site-packages\pyspark\jars"<br/>


<br> Depois fazer o download o "Winutils.exe" referente a versao do spark e criar dentro da pasta do pyspark uma pasta chamada hadoop e dentro dela uma pasta chamada bin e dentro da bin coloca o winutils.exe e o hadoop.dll</br>


```Python

    os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-19"
    os.environ["SPARK_HOME"] = r"C:\databases_Etl\venv\Lib\site-packages\pyspark"



if __name__=='__main__':
    conf = SparkConf()
    conf.set("spark.master","local[*]")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.sql.adaptive.enabled","true")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled","true")
    conf.set("spark.dynamicAllocation.enabled", "false")
    conf.set("spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled","true")
    conf.set("spark.sql.adaptive.skewJoin.enabled","true")
    conf.set("spark.sql.statistics.size.autoUpdate.enabled","true")
    conf.set("spark.sql.inMemoryColumnarStorage.compressed","true")
    conf.set("hive.exec.dynamic.partition", "true")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    conf.set("spark.sql.ansi.enabled","true")
    conf.set('spark.driver.extraClassPath', r"C:\leads\venv\Lib\site-packages\pyspark\mssql-jdbc-9.4.0.jre11.jar")
    conf.set('spark.executor.extraClassPath', r"C:\leads\venv\Lib\site-packages\pyspark\mssql-jdbc-9.4.0.jre11.jar")
    spark = SparkSession.builder\
            .config(conf=conf)\
            .config("spark.sql.warehouse.dir", warehouse_location)\
            .config("spark.sql.catalogImplementation", "hive") \
            .enableHiveSupport() \
            .getOrCreate()
        ...
```
<br>A função Responsavel por abrir a Sessão do Pyspark. O caminho a ser informado no "spark.driver.extraClassPath" é o caminho do arquivo jar do jdbc que deve ser colocado na pasta pyspark Ele deve ser mantido nessa mesma ordem<br/>



```Python

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

```

<br> Consulta de tabela com pyspark. No meu caso eu uso o driver jdbc da Microsoft e ele precisa esta nesse formato ou nao funcionara. a URL do jdbc é a seguinte "'jdbc:sqlserver://Guilherme:1433;database=BIGDATA'"</br>



```Python

def reader_csv(spark: SparkSession) -> None:
    df = spark.read.option("inferSchema",True) \
                 .options(header='True', inferSchema='True', delimiter=';') \
                .csv(r"C:\estimadoresteste\ETL_py_spark_rev5\controllers\exemplo_loja.csv")
    
    NEW = df.select("Nomedoproduto","IDdoproduto","Categoria").filter("Categoria == 'Tecnologia'")
    print(df.show(10))

    NEW.createOrReplaceTempView("ProdutosView")

    spark.sql("CREATE TABLE IF NOT EXISTS produtos (Nomedoproduto String, IDdoproduto Int, Categoria String)")

    spark.sql("INSERT INTO TABLE produtos  SELECT * FROM ProdutosView")

```


<br> Função de leitura de arquivo CSV, faz a leitura, cria tabela e realiza o insert</br>