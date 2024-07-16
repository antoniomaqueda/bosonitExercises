#!/usr/bin/env python
# coding: utf-8

# In[ ]:


'''
En este ejercicio se usarán Kafka, Spark, CSV, JSON y Avro

- Leemos un CSV, lo serializamos en AVRO y lo enviamos a un topic de Kafka, todo ello con Spark

- Posteriormente leemos este topic de Kafka, deserializamos de Avro y lo convertiremos a JSON

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
    .appName("EjercicioFinal") \
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

df.show()

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

# Convertir el DataFrame a formato Avro
df_avro = df.select(to_avro(struct("nombre", "apellido", "sexo", "edad", "peso", "altura"), avro_schema).alias("value"))

# Mostrar el DataFrame en formato Avro
df_avro.show(truncate=False)


# In[5]:


# Ahora lo mandamos a Kafka, al topic "my_topic2"
df_avro.write.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "ejFinal") \
    .save()


# In[16]:


'''
bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic my_topic2 --from-beginning

Pedro
     Pérezm<x���?

María
DíazFFn33�?

MarcoRojoM(|ff�?
CarolinaMartínezf*vH��?
'''


# In[1]:


# Ahora leemos de kafka

df_kafka = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my_topic") \
    .load()

df_kafka.show(truncate=False)


# In[19]:


# Y ahora deserializamos AVRO
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

# Deserializar los datos Avro leídos de Kafka, ene ste caso la columna value
df_avro_deserialized = df_kafka.select(from_avro(col("value"), avro_schema).alias("person"))

# Seleccionar y mostrar los campos deserializados (person)
df_person = df_avro_deserialized.select("person.*")
df_person.show(truncate=False)


# In[ ]:




