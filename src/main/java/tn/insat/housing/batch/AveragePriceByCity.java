package tn.insat.housing.batch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.* ;
import scala.Tuple2;
//import sun.tools.jconsole.JConsole;

import java.util.Arrays;

public class AveragePriceByCity {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: AveragePriceByCity <inputFile> <outputDir>");
            System.exit(1);
        }

        // Create a Spark context
        SparkConf conf = new SparkConf().setAppName("AveragePriceByCity");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the input data
        JavaRDD<String> input = sc.textFile(args[0]);

        // Split each line by comma and filter out header row
        JavaRDD<String[]> rows = input.map(new Function<String, String[]>() {
            @Override
            public String[] call(String line) throws Exception {
                return line.split(",");
            }
        }).filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] parts) throws Exception {
                return !parts[0].equals("id");
            }
        });
        System.out.println("this is the first row");


        String[][] data = rows.collect().toArray(new String[0][]);

        for (int i=0 ;i<4;i++) {
            System.out.println(data[i][4]);
            System.out.println(data[i][20]);
        }

        // Map the data by city and price
        JavaPairRDD<String, Double> cityPrices = rows
                .mapToPair(new PairFunction<String[], String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(String[] parts) throws Exception {
                        return new Tuple2<>(parts[20], Double.parseDouble(parts[4]));
                    }
                });
        System.out.println(cityPrices);
        // Calculate the average price by city
        JavaPairRDD<String, Double> avgPrices = cityPrices
                .groupByKey()
                .mapValues(new Function<Iterable<Double>, Double>() {
                    @Override
                    public Double call(Iterable<Double> prices) throws Exception {
                        double sum = 0;
                        int count = 0;
                        for (Double price : prices) {
                            sum += price;
                            count++;
                        }
                        return sum / count;
                    }
                });

        // Save the results
        avgPrices.saveAsTextFile(args[1]);

        // Stop the Spark context
        sc.stop();
    }
}


