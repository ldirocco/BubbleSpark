import fastdoop.FASTAshortInputFileFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        String inputPath = "data";//args[0];
        int k=31;//Integer.parseInt(args[1]);
        int nBin=120;

        File directory = new File(inputPath);
        Hashtable<String,Integer> file_table=new Hashtable<>(2);
        int file_idx=0;
        for (String fileName : directory.list()){
            file_table.put(fileName,file_idx);
            file_idx+=1;
        }


        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        SparkConf sc = new SparkConf().setAppName("DiscoSnpSpark").setMaster("local[*]");
        sc.set("spark.local.dir", "tmp");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        Configuration inputConf = jsc.hadoopConfiguration();
        inputConf.setInt("look_ahead_buffer_size", 2048);

        //Loading Samples into Memory Using Fastdoop
        JavaPairRDD<Integer, byte[]> samples = jsc.newAPIHadoopFile(inputPath,
                FASTAshortInputFileFormat.class, Text.class, fastdoop.Record.class, inputConf)
                .values()
                .mapToPair(record -> new Tuple2<>(file_table.get(record.getFileName()), record.getValue().getBytes()));


        JavaPairRDD<Integer, Hashtable<Long,Long>> edges=samples
                .mapPartitions(new BinsExtractorFromPartition(k,nBin))
                .mapToPair(t->new Tuple2<>(t._1,t._2))
                .groupByKey()
                .mapToPair(t->{
                    Iterator<Hashtable<Long, Long>> iter = t._2.iterator();
                    Hashtable<Long, Long> bin_frequencies = new Hashtable<>();
                    Hashtable<Long, Long> item;

                    while (iter.hasNext()) {
                        item = iter.next();
                        for (Map.Entry<Long, Long> entry : item.entrySet()) {
                            bin_frequencies.merge(entry.getKey(), entry.getValue(),Long::sum);
                        }
                    }

                    return new Tuple2<>(t._1,bin_frequencies);
                });

        System.out.println(edges.count());
    }
}
