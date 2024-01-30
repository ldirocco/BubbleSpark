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

import java.io.*;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        String inputPath = args[0];
        int k=31;//Integer.parseInt(args[1]);
        int nBin=Integer.parseInt(args[1]);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        SparkConf sc = new SparkConf().setAppName("DiscoSnpSpark").setMaster("local[*]");
        sc.set("spark.local.dir", "tmp");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        Configuration inputConf = jsc.hadoopConfiguration();
        inputConf.setInt("look_ahead_buffer_size", 2048);

        File directory = new File(inputPath);
        String[] files_names=directory.list();
        System.out.println(files_names.length);

        List<String> files_list=new ArrayList<>(files_names.length);
        for(String s:files_names){
            System.out.println(s);
            files_list.add(inputPath+"/"+s);
        }

        JavaPairRDD<Integer, Hashtable<Long,Long>> edgesRDD = jsc.parallelize(files_list)
                .flatMapToPair(path -> {
                    BufferedReader br = new BufferedReader(new FileReader(path));
                    Long kmer;
                    int len;
                    Hashtable<Long, Long>[] frequencies = new Hashtable[nBin];
                    KmerTool ktool;
                    String read;

                    
                    while ((read = br.readLine()) != null) {
                        if (read.startsWith(">"))
                            continue;
                        len=read.length();
                        ktool = new KmerTool(read.getBytes(), k);
                        for (int i = 0; i < len - (k-1); i++) {
                            kmer = ktool.nextKmerCan();
                            int idBin = Math.abs(Long.hashCode(kmer) % nBin);
                            if (frequencies[idBin] == null)
                                frequencies[idBin] = new Hashtable<>();
                            frequencies[idBin].merge(kmer, 1l, Long::sum);
                        }
                    }

                    List<Tuple2<Integer, Hashtable<Long, Long>>> kmers = new ArrayList<>();
                    for (int idBin = 0; idBin < nBin; idBin++)
                        if (frequencies[idBin] != null)
                            kmers.add(new Tuple2<>(idBin,frequencies[idBin]));
                    return kmers.iterator();
                })
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

        System.out.println(edgesRDD.count());

    }
}
