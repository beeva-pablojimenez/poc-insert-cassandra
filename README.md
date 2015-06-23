# POC-insert-cassandra
Pre-requisitos:

- Instalacion de Apache Cassandra (apache-cassandra-2.1.4)
- Scala 2.10
- IDE Scala
- JDK 1.7

Cómo hacerlo andar:
- Descargar el código y desplegarlo en el IDE Scala (Usar JDK 1.7 y Scala 2.10)
- Arrancar Cassandra con comando ./cassandra -f
- Arrancar la CQLSH con comando ./cqlsh
- Crear el keyspace y la tabla (ver fichero /src/main/resorces/bbdd.cql)
- Para arrancar la inserción de datos ejecutar com.beeva.poc.cassandra.massive_insertion.Run
	(meter como parametros de JVM -Dakka.loglevel=DEBUG -Dakka.actor.debug.receive=on)
- Para consultar datos con Spark ejecutar com.beeva.poc.cassandra.massive_insertion.CassandraSpark


