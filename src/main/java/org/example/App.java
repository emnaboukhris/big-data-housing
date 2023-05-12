package org.example;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

import javax.imageio.ImageIO;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.RangeType;
import org.jfree.data.category.DefaultCategoryDataset;

public class App {

    public static void main(String[] args) throws IOException {

        List<String> cities = Arrays.asList("Paris", "London", "New York", "Tokyo");
        List<Double> prices = Arrays.asList(200000.0, 250000.0, 300000.0, 180000.0);

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        for (int i = 0; i < cities.size(); i++) {

            dataset.addValue(prices.get(i), "Average Price by City", cities.get(i));
            System.out.println(dataset.getValue(0,0));

        }

        JFreeChart chart = ChartFactory.createBarChart("Average Price by City", "City", "Price", dataset, PlotOrientation.VERTICAL, false, true, false);
        CategoryPlot plot = chart.getCategoryPlot();
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setSeriesPaint(0, Color.BLUE);
        NumberAxis rangeAxis = (NumberAxis) chart.getCategoryPlot().getRangeAxis();
        rangeAxis.setRangeType(RangeType.POSITIVE);

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
