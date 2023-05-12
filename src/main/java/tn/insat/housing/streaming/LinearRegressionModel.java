//package tn.insat.housing.streaming;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.ml.evaluation.RegressionEvaluator;
//import org.apache.spark.ml.feature.VectorAssembler;
//import org.apache.spark.ml.regression.LinearRegression;
//import org.apache.spark.ml.regression.LinearRegressionModel;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//import java.io.IOException;
//
//public class LinearRegressionmodel {
//
//    public static void main(String[] args) {
//
//        // Create a Spark session
//        SparkConf sparkConf = new SparkConf().setAppName("LinearRegressionmodel");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        SparkSession spark = SparkSession.builder().appName("LinearRegressionmodel").getOrCreate();
//
//        // Load the dataset as a DataFrame
//        String inputPath = "/path/to/dataset";
//        Dataset<Row> data = spark.read().format("csv")
//                .option("header", "true")
//                .option("inferSchema", "true")
//                .load(inputPath);
//
//        // Split the data into training and testing sets
//        Dataset<Row>[] splits = data.randomSplit(new double[] {0.7, 0.3});
//        Dataset<Row> trainingData = splits[0];
//        Dataset<Row> testingData = splits[1];
//
//        // Define the feature vector
//        VectorAssembler assembler = new VectorAssembler()
//                .setInputCols(new String[] {"feature1", "feature2", "feature3"})
//                .setOutputCol("features");
//        trainingData = assembler.transform(trainingData);
//        testingData = assembler.transform(testingData);
//
//        // Train the model using the training set
//        LinearRegression lr = new LinearRegression()
//                .setMaxIter(10)
//                .setRegParam(0.3)
//                .setElasticNetParam(0.8);
//        LinearRegressionModel model = lr.fit(trainingData);
//
//        // Evaluate the model on the testing set
//        Dataset<Row> predictions = model.transform(testingData);
//        RegressionEvaluator evaluator = new RegressionEvaluator()
//                .setLabelCol("label")
//                .setPredictionCol("prediction")
//                .setMetricName("rmse");
//        double rmse = evaluator.evaluate(predictions);
//        System.out.println("Root Mean Squared Error = " + rmse);
//
//        // Save the trained model to disk
//        String outputPath = "/path/to/trained/model";
//        try {
//            model.write().save(outputPath);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        // Stop the Spark context
//        sc.stop();
//    }
//}
