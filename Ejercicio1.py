#!/usr/bin/env python
# coding: utf-8

# In[ ]:


'''
En este ejercicio se usarán Kafka, Spark, CSV y Avro

- Queremos serializar un CSV con Avro usando spark, y enviarlo a un topic de Kafka

- Posteriormente al revés, consumimos de un topic de Kafka un mensaje serializado en Avro, y con Spark lo deserializamos y leemos

El CSV es:

nombre;apellido;sexo;edad;peso;altura
Pedro;Pérez;m;30;60;1.70
María;Díaz;F;35;55;1.65
Marcos;Rojo;M;20;62;1.80
Carolina;Martínez;f;21;59;1.71

'''


# In[1]:


from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro, to_avro
import findspark
import pandas as pd
from deltalake.writer import write_deltalake

# Inicializar findspark
findspark.init()


# In[2]:


# Crear la sesión de Spark
# Necesitaremos incluir las dependencias de AVRO y Kafka en spark.jars.packages
spark = SparkSession.builder \
    .appName("ReadingAvroFromKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,org.apache.spark:spark-avro_2.13:3.5.1") \
    .getOrCreate()


# In[4]:


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, struct

# Definir el esquema del CSV
csv_schema = StructType([
    StructField("nombre", StringType(), True),
    StructField("apellido", StringType(), True),
    StructField("sexo", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("peso", IntegerType(), True),
    StructField("altura", FloatType(), True)
])

# Leer el CSV en un DataFrame
df = spark.read.csv("/home/bosonituser/Desktop/CSVFiles/mydoc.csv", header=True, schema=csv_schema, sep=";")

# Lo mostramos para comprobar que se ha realizado correctamente
df.show()


# In[5]:


# Definir el esquema Avro en formato JSON
avro_schema = '''
{
  "type": "record",
  "name": "Person",
  "fields": [
    {"name": "nombre", "type": ["null", "string"], "default": null},
    {"name": "apellido", "type": ["null", "string"], "default": null},
    {"name": "sexo", "type": ["null", "string"], "default": null},
    {"name": "edad", "type": ["null", "int"], "default": null},
    {"name": "peso", "type": ["null", "int"], "default": null},
    {"name": "altura", "type": ["null", "float"], "default": null}
  ]
}
'''

# Serializar DataFrame a formato Avro (to_avro)
df_avro = df.select(to_avro(struct("nombre", "apellido", "sexo", "edad", "peso", "altura"), avro_schema).alias("value"))

# Mostrar el DataFrame en formato Avro
df_avro.show(truncate=False)


# In[6]:


# Deserializar de formato Avro el DataFrame (from_avro)
df_back = df_avro.select(from_avro(col("value"), avro_schema).alias("person"))

# Comporbar que se ha realizado correctamente la conversión
df_back.select("person.*").show(truncate=False)


# In[7]:


# Ahora lo mandamos a Kafka, al topic "my_topic2"
df_avro.write.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "ej1") \
    .save()


# In[16]:


# Comprobamos lo que llega en la terminal del ordenador

'''
bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic ej1 --from-beginning

Pedro
     Pérezm<x���?

María
DíazFFn33�?

MarcoRojoM(|ff�?
CarolinaMartínezf*vH��?

'''


# In[8]:


# Ahora leemos de kafka a un DataFrame, que estará serializado en Avro

df_kafka = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ej1") \
    .load()

df_kafka.show(truncate=False)


# In[9]:


# Y ahora deserializamos de AVRO el dataFrame que nos ha llegado para poder ver los datos
avro_schema = '''
{
  "type": "record",
  "name": "Person",
  "fields": [
    {"name": "nombre", "type": ["null", "string"], "default": null},
    {"name": "apellido", "type": ["null", "string"], "default": null},
    {"name": "sexo", "type": ["null", "string"], "default": null},
    {"name": "edad", "type": ["null", "int"], "default": null},
    {"name": "peso", "type": ["null", "int"], "default": null},
    {"name": "altura", "type": ["null", "float"], "default": null}
  ]
}
'''

# Deserializar los datos Avro leídos de Kafka, en ete caso la columna value, que es donde está el contenido de los registros (ver df_kafka)
df_avro_deserialized = df_kafka.select(from_avro(col("value"), avro_schema).alias("person"))

# Seleccionar y mostrar los campos deserializados
df_person = df_avro_deserialized.select("person.*")
df_person.show(truncate=False)


# In[ ]:




