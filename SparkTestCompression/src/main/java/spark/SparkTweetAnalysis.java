package spark;


import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.chaiware.app.TextToEmotion;
import scala.Function1;
import scala.Tuple2;

public final class SparkTweetAnalysis {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("<folder input>");
            return;
        }
        SparkSession spark = SparkSession.builder().appName("SparkTweetAnalysisCheckCompression").getOrCreate();
        SparkContext context=spark.sparkContext();
        FileSystem fs = FileSystem.get(context.hadoopConfiguration());
        RDD<Tuple2<LongWritable,Text>> data= context.newAPIHadoopFile(args[0], LzoTextInputFormat.class, LongWritable.class, Text.class, new Configuration());
        spark.read().json();


        Dataset<String> dataString =  spark.createDataset(data.toJavaRDD().map(x->x._2.toString()).rdd(), Encoders.bean(String.class));
        Dataset<Row> dsr=spark.read().json(dataString);
        Dataset<Row> dsr2=dsr.select("text");
        dsr2.show();

    }


}