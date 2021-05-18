# Scala Case Study
### Overview
A simple application split into two components: a publisher that generates device data and produces it into Kafka and a
subscriber which consumes device data from Kafka, aggregates the data, and also inserts it into a Postgres DB.

Each component has its own Actor System and is made up of several different actors, one to handle each task.

### Running the app
In order to run the app, you will need to make sure you have access to a working Kafka Cluster and also an instance of
PostgresDB. The application.conf included in this project assumes that these services are running on localhost with default ports.
### Postgres Schema
```
CREATE TABLE DEVICE_READINGS (
    ID        UUID      NOT NULL,
    VALUE     FLOAT(6)  NOT NULL,
    UNIT      TEXT      NOT NULL,
    TIMESTAMP TIMESTAMP NOT NULL,
    VERSION   SMALLINT  NOT NULL
 );
```