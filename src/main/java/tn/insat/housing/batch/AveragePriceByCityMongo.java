package tn.insat.housing.batch;

import com.mongodb.MongoClientURI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.IOException;

public class AveragePriceByCityMongo {
    private static Table table;
    private static String tableName="housing";

    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            System.err.println("Usage: AveragePriceByCityHBase <inputFile> <tableName>");
            System.exit(1);
        }

        // Create a Spark context
        SparkConf conf = new SparkConf().setAppName("AveragePriceByCityHBase").setMaster("local[4]");
        conf.set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);
        conf.set("spark.driver.maxResultSize", "2g");
        conf.set("spark.akka.frameSize", "1000");

        // Load the input data
        JavaRDD<String> input = sc.textFile(args[0]);

        // Split each line by comma and filter out header row
        JavaRDD<String[]> rows = input.map(line -> line.split(","))
                .filter(parts -> !parts[0].equals("id"));

        // Map the data by city and price
        JavaPairRDD<String, Double> cityPrices = rows
                .mapToPair(parts -> new Tuple2<>(parts[20], Double.parseDouble(parts[4])));

        // Calculate the average price by city
        JavaPairRDD<String, Double> avgPrices = cityPrices
                .groupByKey()
                .mapValues(prices -> {
                    double sum = 0;
                    int count = 0;
                    for (Double price : prices) {
                        sum += price;
                        count++;
                    }
                    return sum / count;
                });

        // MongoDB configuration
        MongoClientURI uri = new MongoClientURI("mongodb://localhost:27017");
        MongoClient mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase("housing");
        MongoCollection<Document> collection = database.getCollection("city_prices");


        // Write the data to MongoDB
        avgPrices.foreach(cityPrice -> {
            String city = cityPrice._1;
            Double price = cityPrice._2;
            Document doc = new Document("city", city)
                    .append("avg_price", price);
            collection.insertOne(doc);
        });
        // Stop the Spark context
        sc.stop();
    }
}
