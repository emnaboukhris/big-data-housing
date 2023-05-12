package tn.insat.housing.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import scala.Tuple2;
import tn.insat.housing.nosql.HbaseSparkProcess;


import java.io.IOException;

public class AveragePriceByCityHBase {
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
                .mapToPair(parts -> new Tuple2<>(parts[2], Double.parseDouble(parts[1])));

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

        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf(tableName));

        // Iterate over the avgPrices RDD and add each city's average price to the HBase table
        avgPrices.foreach(cityPrice -> {
            String city = cityPrice._1;
            Double price = cityPrice._2;
            Put put = new Put(Bytes.toBytes(city));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("avg_price"), Bytes.toBytes(price.toString()));
            table.put(put);
        });
        // Stop the Spark context
        sc.stop();
    }
}
