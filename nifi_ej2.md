# Parte de NIFI en Ejercicio2

## Enunciado
Queremos serializar un CSV con Avro y enviarlo a un topic de Kafka, todo ello con NIFI

### Pasos

1. Importar el CSV -> GetFile

![Processor](imagenes/getFile.JPG)

![Processor Properties](imagenes/getFileProp.JPG)


3. Añadir extensión .avro -> UpdateAttribute

![Processor](imagenes/updateAttribute.JPG)

![Processor Properties](imagenes/updateAttributeProp.JPG)


4. Enviar a un topic de Kafka -> PublishKafkaRecord

![Processor](imagenes/publishKafkaRecord.JPG)

![Processor Properties](imagenes/publishKafkaRecordProp.JPG)


