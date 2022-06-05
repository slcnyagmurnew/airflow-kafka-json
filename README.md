# airflow-kafka-json

This study is working with **data/counts.json** file.

Data is divided into small one data json files and saved into Postgre DB. They are sent to Kafka broker from Postgre with given interval to read as stream data.

Read data is gone to **data/garbage** directory. Data is updated in both Kafka and Postgre DB. 

Up the Docker containers with run:

```
docker-compose up -d
```
