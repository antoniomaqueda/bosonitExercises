# Enunciados y preparación 

## Ejercicio 1

En este ejercicio se usarán Kafka, Spark, CSV y Avro

- Queremos serializar un CSV con Avro y enviarlo a un topic de Kafka, todo ello con Spark

- Posteriormente al revés, consumimos de un topic de Kafka un mensaje serializado en Avro, y con Spark lo deserializamos y leemos



## Ejercicio 2

En este ejercicio se usarán Kafka, Spark, NIFI, CSV y Avro

- Queremos serializar un CSV con Avro y enviarlo a un topic de Kafka, todo ello con NIFI

- Posteriormente consumimos de ese topic de Kafka el CSV serializado en Avro, y con Spark lo deserializamos y leemos



## Ejercicio 3

En este ejercicio se usarán Kafka, Spark, NIFI, CSV, JSON y Avro

- Leemos un CSV, lo serializamos en AVRO y lo enviamos a un topic de Kafka, todo ello con Spark

- Posteriormente leemos este topic de Kafka, deserializamos de Avro y lo convertiremos a JSON, todo ello con NIFI


### Lo referente a la configuración de NIFI se encuentra en el fichero "nifi.md"
