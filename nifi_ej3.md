# Parte de NIFI en Ejercicio3

## Enunciado
Queremos leer un topic de Kafka, deserializarlo de Avro y convertirlo a JSON, todo ello con NIFI

![Ruta](imagenes/ej3.JPG)

## Pasos

### 1. COnsumir un topic de Kafka y transformarlo a JSON

![Processor](imagenes/consumeKafka.JPG)

- Indicar el topic al que enviaremos los datos, además de la localización de los brokers

![Processor Properties](imagenes/consumeKafkaProp1.JPG)
![Processor Properties](imagenes/consumeKafkaProp2.JPG)

- Debemos añadir controller services en Record reader (AvroReader) y Record Writer (AvroRecordSetWriter), y los activamos:


- Indicamos la carpeta dónde se encuentra el CSV




### 2. Añadir extensión .avro -> UpdateAttribute

![Processor](imagenes/updateAttribute.JPG)

![Processor Properties](imagenes/updateAttributeProp.JPG)

- Añadimos property "Filename" para cambiar el nombre: fichero.csv -> fichero.avro
  
- Añadimos property "schema.name" para incluir el esquema AVRO que se encuentra en el codigo




### 3. Enviar a un topic de Kafka -> PublishKafkaRecord

![Processor](imagenes/publishKafkaRecord.JPG)

![Processor Properties](imagenes/publishKafkaRecordProp.JPG)

- Indicar el topic al que enviaremos los datos, además de la locaclización de los brokers


- Debemos añadir controller services en Record reader (CSVReader1) y Record Writer (AvroRecordSetWriter), y los activamos:

![Processor Properties](imagenes/csvReader.JPG)

- Indicar el uso de "header" y el ";" como separador

![Processor Properties](imagenes/AvroRecordSetWriter.JPG)

- Usar el shcema.name especificado anteriormente

  
