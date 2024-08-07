# Parte de NIFI en Ejercicio2

## Enunciado
Queremos serializar un CSV con Avro y enviarlo a un topic de Kafka, todo ello con NIFI

![Ruta](imagenes/ej1.JPG)

## Pasos

### 1. Importar el CSV -> GetFile

![Processor](imagenes/getFile.JPG)

![Processor Properties](imagenes/getFileProp.JPG)

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

  
