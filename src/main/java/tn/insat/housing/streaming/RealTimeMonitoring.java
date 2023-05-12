//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package tn.insat.housing.streaming;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

public class RealTimeMonitoring {
    private static final Pattern SPACE = Pattern.compile(" ");

    private RealTimeMonitoring() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: RealTimeMonitoring <zkQuorum> <group> <topics> <numThreads> <outputfile>");
            System.exit(1);
        }

        SparkConf sparkConf = (new SparkConf()).setAppName("RealTimeMonitoring");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000L));
        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap();
        String[] topics = args[2].split(",");
        String[] var6 = topics;
        int var7 = topics.length;

        for(int var8 = 0; var8 < var7; ++var8) {
            String topic = var6[var8];
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
        JavaDStream<String> lines = messages.map(Tuple2::_2);
        lines.print();
        lines.foreachRDD((rdd) -> {
            rdd.foreachPartition((partitionOfRecords) -> {
                FileWriter fileWriter = new FileWriter(args[4], true);
                CSVWriter csvWriter = new CSVWriter(fileWriter);

                while(partitionOfRecords.hasNext()) {
                    String[] record = ((String)partitionOfRecords.next()).split(",");
                    csvWriter.writeNext(record);
                }

                csvWriter.close();
                fileWriter.close();
            });
        });
        jssc.start();
        jssc.awaitTermination();
    }
}