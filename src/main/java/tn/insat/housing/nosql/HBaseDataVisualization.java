package tn.insat.housing.nosql;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.RangeType;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.imageio.ImageIO;

public class HBaseDataVisualization {
    private static Table table;
    private static String tableName="housing";
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(TableName.valueOf(tableName));


        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("cf"));
        DecimalFormat df = new DecimalFormat("#.00");
        List<String> cities = new ArrayList<>();
        List<Double> prices = new ArrayList<>();
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("avg_price"));
        ResultScanner scanner = table.getScanner(scan);
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        System.out.println("hello") ;
        System.out.println(scanner) ;
        System.out.println("scanner") ;


        for (Result result : scanner) {
            NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("cf"));
            String city = Bytes.toString(result.getRow());
            String pricet = Bytes.toString(map.get(Bytes.toBytes("avg_price")));
            System.out.println("offffffffffffffffffffffffff");
            System.out.println(pricet);

            Double price = Double.parseDouble(pricet);
            cities.add(city);
            prices.add(price);  // add the actual value of avg_price
        }


// Print the lists to verify the values
        System.out.println(cities);
        System.out.println(prices);


        for (int i = 0; i < cities.size(); i++) {

            dataset.addValue(prices.get(i), "Average Price by City", cities.get(i));

        }

//        for (int i = 0; i < dataset.getRowCount(); i++) {
//            for (int j = 0; j < dataset.getColumnCount(); j++) {
//                String rowKey = (String) dataset.getRowKey(i);
//                String columnKey = (String) dataset.getColumnKey(j);
//                Double value = dataset.getValue(i, j).doubleValue();
//                System.out.println("Category: " + columnKey + ", serie: " + rowKey + ", value: " + value);
//            }
//        }



        JFreeChart chart = ChartFactory.createBarChart("Average Price by City", "City", "Price", dataset, PlotOrientation.VERTICAL, false, true, false);
        CategoryPlot plot = chart.getCategoryPlot();
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setSeriesPaint(0, Color.BLUE);
        NumberAxis rangeAxis = (NumberAxis) chart.getCategoryPlot().getRangeAxis();
        rangeAxis.setRangeType(RangeType.POSITIVE);
        String outputPath = "/user/hadoop/housing.png"; // Change this to the desired HDFS location
        Path outputFilePath = new Path(outputPath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        BufferedImage image = chart.createBufferedImage(3000, 2000);
// Write the BufferedImage to a file
        File outputFile = new File("output.png");
        try {
            ImageIO.write(image, "png", outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
