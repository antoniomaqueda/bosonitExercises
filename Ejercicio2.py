#!/usr/bin/env python
# coding: utf-8

# In[ ]:


'''
En este ejercicio se usarán Kafka, Spark, CSV y Avro

- Queremos serializar un CSV con Avro y enviarlo a un topic de Kafka, todo ello con NIFI

- Posteriormente consumimos de ese topic de Kafka el CSV serializado en Avro, y con Spark lo deserializamos y leemos

El CSV es:

nombre;apellido;sexo;edad;peso;altura
Pedro;Pérez;m;30;60;1.70
María;Díaz;F;35;55;1.65
Marcos;Rojo;M;20;62;1.80
Carolina;Martínez;f;21;59;1.71

'''


# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro
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


# In[3]:


# Tendremos en kafka preparado el csv serializado en Avro, ésto se habrá realizado previamente en NIFI
# Leemos del topic que hemos especificado en el processor de NIFI

df_kafka = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "csv_avro2") \
    .load()

df_kafka.show(truncate=False)


# In[4]:


# Definir el esquema Avro en formato JSON

avro_schema = '''
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
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

# Deserializar los datos Avro leídos de Kafka
df_avro_deserialized = df_kafka.select(from_avro(col("value"), avro_schema).alias("person"))

# Seleccionar y mostrar los campos deserializados
df_person = df_avro_deserialized.select("person.*")
df_person.show(truncate=False)


# In[ ]:




