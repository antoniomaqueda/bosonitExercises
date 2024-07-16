# Parte de NIFI en Ejercicio3

## Enunciado
Queremos leer un topic de Kafka, deserializarlo de Avro y convertirlo a JSON, todo ello con NIFI

![Ruta](imagenes/ej3.JPG)

## Pasos

### 1. Consumir un topic de Kafka y transformarlo a JSON

![Processor](imagenes/consumeKafka.JPG)

- Indicar el topic al que enviaremos los datos, además de la localización de los brokers

![Processor Properties](imagenes/consumeKafkaProp1.JPG)
![Processor Properties](imagenes/consumeKafkaProp2.JPG)

- Debemos añadir controller services en Record reader (AvroReader) y Record Writer (jsonRecordSetWriter), y los activamos:

![Processor Properties](imagenes/avroReaderr.JPG)

- Usamos el schema.name y el esquema que encontramos en el código

![Processor Properties](imagenes/jsonRecordSetWriter.JPG)




### 2. Colocamos el JSON en un directorio -> PutFile

![Processor](imagenes/putFileProp.JPG)




  
