package tn.insat.housing.nosql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.util.Iterator;

import scala.Tuple2;

public class HBaseWriter {
    private static final String HBASE_TABLE_NAME = "housing";
    private static final String HBASE_COLUMN_FAMILY = "cf";

    public static void writeToHBase(JavaDStream<Tuple2<String, Double>> stream) {
        stream.foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, Double>>>() {
            @Override
            public void call(JavaRDD<Tuple2<String, Double>> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Double>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Double>> iterator) throws Exception {
                        Configuration config = HBaseConfiguration.create();
                        Connection connection = ConnectionFactory.createConnection(config);
                        try {
                            TableName tableName = TableName.valueOf(HBASE_TABLE_NAME);
                            byte[] columnFamilyBytes = HBASE_COLUMN_FAMILY.getBytes();
                            while (iterator.hasNext()) {
                                Tuple2<String, Double> record = iterator.next();
                                String city = record._1();
                                Double avgPrice = record._2();
                                Put put = new Put(city.getBytes());
                                put.addColumn(columnFamilyBytes, "avg_price".getBytes(), avgPrice.toString().getBytes());
                                connection.getTable(tableName).put(put);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            connection.close();
                        }
                    }
                });
            }
        });
    }
}
