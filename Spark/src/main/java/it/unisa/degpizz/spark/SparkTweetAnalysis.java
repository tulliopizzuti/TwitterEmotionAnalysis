package it.unisa.degpizz.spark;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.chaiware.app.TextToEmotion;
import scala.Tuple2;
import java.io.IOException;

public final class SparkTweetAnalysis {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("<folder input> <folder output>");
            return;
        }
        SparkSession spark = SparkSession.builder().appName("SparkTweetAnalysis").getOrCreate();
        FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        fs.delete(new Path(args[1]), true);
        spark
                .read()
                .json(args[0])
                .select("text")
                .toJSON()
                .toJavaRDD()
                .mapToPair(s -> new Tuple2<>(TextToEmotion.textToEmotionString(s), 1))
                .reduceByKey((a, b) -> a + b)
                .saveAsTextFile(args[1]);
    }


}