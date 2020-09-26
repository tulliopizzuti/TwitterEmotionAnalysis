package it.unisa.degpizz.spark;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.chaiware.app.TextToEmotion;
import scala.Tuple2;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class SparkTweetAnalysis {
    private static final List<String> LANGTOPARSE = Arrays.asList("en");

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
                .filter((FilterFunction<Row>) row ->
                        LANGTOPARSE.contains(row.getString(row.fieldIndex("lang"))))
                .select("text")
                .toJSON()
                .toJavaRDD()
                .mapToPair(s -> new Tuple2<>(TextToEmotion.textToEmotionString(s), 1))
                .reduceByKey((a, b) -> a + b)
                .saveAsTextFile(args[1]);



    }


}