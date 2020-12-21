package it.unisa.degpizz.spark;


import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.chaiware.app.TextToEmotion;
import scala.Tuple2;

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

        RDD<Tuple2<LongWritable, Text>> data= context.newAPIHadoopFile(args[0], LzoTextInputFormat.class, LongWritable.class, Text.class, new Configuration());
        Dataset<Row> dsr=spark.read().json(data.toJavaRDD().map(x->x._2().toString()));
        Dataset<Row> dsr2=dsr.select("text");
        Dataset<String> dsrs=dsr2.toJSON();
        JavaRDD<String> rddS=dsrs.toJavaRDD();
        rddS.mapToPair(s -> new Tuple2<>(TextToEmotion.textToEmotionString(s), 1))
                .reduceByKey((a, b) -> a + b)
                .saveAsTextFile(args[1]);
    }


}