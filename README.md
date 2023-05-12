# big-data-housing
Project idea:
Real-time monitoring of housing listings (tracking new, modified, and deleted listings), analysis of market trends (tracking changes in prices and housing features over time), and generating reports based on the USA Housing Listings dataset from Kaggle.

Architecture:
Using a lambda architecture for processing both batch and streaming data, storing data in a storage layer for batch analysis, and using a presentation layer to visualize results.
![test](https://github.com/emnaboukhris/big-data-housing/assets/79046370/45d3f2bf-422f-4f21-9e47-4871863cf912)

Commands:
- Start Hadoop: `docker exec -it hadoop-master bash` then `./start-hadoop.sh`
- Start Kafka and Zookeeper: `./start-kafka-zookeeper.sh`
- Create Kafka topic: `kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic housing`
- Run streaming consumer: `spark-submit --class tn.insat.housing.streaming.RealTimeMonitoring --master local[2] housing-1-jar-with-dependencies.jar localhost:2181 test housing 1 housing/housingStr.csv  >> strout`
- Run streaming producer: `javac -cp "$KAFKA_HOME/libs/*":. HousingProducer.java` then `java -cp "$KAFKA_HOME/libs/*":. HousingProducer housing housing.csv`
- Run batch processing for average price by city: `spark-submit --class tn.insat.housing.batch.AveragePriceByCity --master local --yarn driver-memory 4g --executor-memory 2g --executor-cores 1 housing-1-jar-with-dependencies.jar housing/housing.csv outputbatch` then `more outputBatch/part-00000`
- Run batch processing with HBase integration: `spark-submit --class tn.insat.housing.batch.AveragePriceByCityHBase --master yarn --deploy-mode client housing-1-jar-with-dependencies.jar housing/housingBig.csv housing`
- Visualize results with HBase data: `spark-submit --class tn.insat.housing.nosql.HBaseDataVisualization --master yarn --deploy-mode client --jars /usr/local/hadoop/share/hadoop/common/lib/jfreechart-1.0.19.jar,/usr/local/hadoop/share/hadoop/common/lib/jcommon-1.0.23.jar housing-1-jar-with-dependencies.jar` then `docker cp hadoop-master:/root/output.png Desktop/graph.png`

![output](https://github.com/emnaboukhris/big-data-housing/assets/79046370/37d1a9b6-4383-4490-9b89-d5c56e2de7a3)
