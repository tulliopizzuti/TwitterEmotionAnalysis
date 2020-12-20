package it.unisa.degpizz.spark;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.chaiware.app.TextToEmotion;
import scala.Tuple2;
import java.io.IOException;

public final class SparkTweetAnalysis {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("<folder input> <folder output>");
            return;
        }
        SparkSession spark = SparkSession.builder().appName("SparkTweetAnalysis").getOrCreate();
        SparkContext context=spark.sparkContext();
        FileSystem fs = FileSystem.get(context.hadoopConfiguration());
        fs.delete(new Path(args[1]), true);



        DataFrameReader dfr=spark.read();
                Dataset<Row> dsr=dfr.json(args[0]);
        Dataset<Row> dsr2=dsr.select("text");
        Dataset<String> dsrs=dsr2.toJSON();
        JavaRDD<String> rddS=dsrs.toJavaRDD();
        rddS.mapToPair(s -> new Tuple2<>(TextToEmotion.textToEmotionString(s), 1))
                .reduceByKey((a, b) -> a + b)
                .saveAsTextFile(args[1]);
    }


}