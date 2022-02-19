import com.google.common.collect.Iterables;
import fastdoop.FASTAshortInputFileFormat;
import fastdoop.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import scala.collection.immutable.List;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import java.io.IOException;
import java.util.ArrayList;

public class Main {
        public static void main(String[] args) throws IOException {
            String inputPath1 = args[0];
            String inputPath2 = args[1];
            int k = Integer.parseInt(args[2]);
            int c = Integer.parseInt(args[3]);
            String master=args[4];


            Logger.getLogger("org").setLevel(Level.ERROR);
            Logger.getLogger("akka").setLevel(Level.ERROR);
            SparkConf sc = new SparkConf().setAppName("DiscoSnpSpark").setMaster(master);
            sc.set("spark.local.dir", "tmp");
            JavaSparkContext jsc = new JavaSparkContext(sc);
            Configuration inputConf = jsc.hadoopConfiguration();
            inputConf.setInt("look_ahead_buffer_size", 2048);


            // Loading input
            JavaPairRDD<Text, Record> dSequences1 = jsc.newAPIHadoopFile(inputPath1,
                    FASTAshortInputFileFormat.class, Text.class, fastdoop.Record.class, inputConf);
            JavaPairRDD<Text, fastdoop.Record> dSequences2 = jsc.newAPIHadoopFile(inputPath2,
                    FASTAshortInputFileFormat.class, Text.class, Record.class, inputConf);


            // We drop the keys of the new RDD since they are not used, than a PairRDD (ID, sequence) is created
            JavaPairRDD<String, String> dSequencesA = dSequences1.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));//.repartition(jsc.defaultParallelism());
            JavaPairRDD<String, String> dSequencesB = dSequences2.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));//.repartition(jsc.defaultParallelism());


            JavaRDD<String> dSeqA = dSequencesA.map(record -> record._2);
            JavaRDD<String> dSeqB = dSequencesB.map(record -> record._2);


            // (k+1)-mers
            JavaRDD<String> dK1mersA = dSeqA.map(new getK1mers(k+1)).flatMap(ArrayList::iterator);
            JavaRDD<String> dK1mersB = dSeqB.map(new getK1mers(k+1)).flatMap(ArrayList::iterator);

            JavaRDD<Kmer> dRep1A = dK1mersA.map(kmer -> new Kmer(kmer.getBytes()));
            JavaRDD<Kmer> dRep1B = dK1mersB.map(kmer -> new Kmer(kmer.getBytes()));


            //Delete (k+1)-mers with freq<c
            JavaPairRDD<Kmer, Integer> dRep1freqA = dRep1A.mapToPair(kmer-> new Tuple2<>(kmer, 1)).reduceByKey(Integer::sum);
            JavaPairRDD<Kmer, Integer> dRep1freqB = dRep1B.mapToPair(kmer-> new Tuple2<>(kmer, 1)).reduceByKey(Integer::sum);

            JavaPairRDD<Kmer, Integer> dRep1freqARed = dRep1freqA.filter(kmer -> kmer._2>=c);
            JavaPairRDD<Kmer, Integer> dRep1freqBRed = dRep1freqB.filter(kmer -> kmer._2>=c);


            //Graph Creation
            JavaRDD<Tuple3<Kmer, Kmer, Integer>> dPairedKmersA = dRep1freqARed.map(new getPairs(k+1));
            JavaRDD<Tuple3<Kmer, Kmer, Integer>> dPairedKmersB = dRep1freqBRed.map(new getPairs(k+1));
            JavaPairRDD<Tuple2<Kmer, Kmer>, Integer> dPairedKmers = dPairedKmersA.union(dPairedKmersB).mapToPair(kmer -> new Tuple2<>(new Tuple2<>(kmer._1(),kmer._2()),kmer._3())).reduceByKey(Integer::sum);

            //Sample A
            JavaPairRDD<Kmer, Integer> dKmers1A = dPairedKmersA.mapToPair(kmer -> new Tuple2<>(kmer._1(),1));
            JavaPairRDD<Kmer, Integer> dKmers2A = dPairedKmersA.mapToPair(kmer -> new Tuple2<>(kmer._2(),1));
            JavaPairRDD<Kmer, Integer> dKmersA = dKmers1A.union(dKmers2A).reduceByKey(Integer::sum);

            //Sample B
            JavaPairRDD<Kmer, Integer> dKmers1B = dPairedKmersB.mapToPair(kmer -> new Tuple2<>(kmer._1(),1));
            JavaPairRDD<Kmer, Integer> dKmers2B = dPairedKmersB.mapToPair(kmer -> new Tuple2<>(kmer._2(),1));
            JavaPairRDD<Kmer, Integer> dKmersB = dKmers1B.union(dKmers2B).reduceByKey(Integer::sum);


            JavaRDD<Edge<Integer>> dEdges = dPairedKmers.map(kmer -> new Tuple3<>(kmer._1._1, kmer._1._2, kmer._2)).map(kmer -> new Edge<>(kmer._1().kmer, kmer._2().kmer, kmer._3()));
            JavaRDD<Tuple2<Object, Kmer>> dVertices = dKmersA.union(dKmersB).reduceByKey(Integer::sum).map(kmer->new Tuple2<>(kmer._1().kmer,kmer._1));

            ClassTag<Integer> integerTag = ClassTag$.MODULE$.apply(Integer.class);
            ClassTag<Kmer> kmerTag = ClassTag$.MODULE$.apply(Kmer.class);

            Kmer kmer = new Kmer("AAAAAA".getBytes());

            Graph<Kmer, Integer> graph = Graph.apply(dVertices.rdd(), dEdges.rdd(), kmer,
                    StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                    kmerTag, integerTag);


            //Bubbles Detectiom
            JavaRDD<Tuple2<Object, Tuple5<Kmer, List<Object>, String, Object, Integer>>> sol = Bubbles.SNP(graph, k).toJavaRDD();
            JavaRDD<Tuple2<String, String>> seq = sol.map(v -> v._2._3().split(" ")).map(v->new Tuple2<>(v[0],v[1]));
            JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> seqCanon = seq.mapToPair(new getCanonicalBubble(k));
            JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<String, String>>> BubbleCanonAll = seqCanon.groupByKey();
            //System.out.println(BubbleCanonAll.take(6));
            JavaRDD<Tuple2<String, String>> BubbleDouble = BubbleCanonAll.filter(t -> Iterables.size(t._2) >= 2).keys();
            JavaRDD<Tuple2<String, String>> BubbleSingle = BubbleCanonAll.filter(t -> Iterables.size(t._2) == 1).values().map(t -> Iterables.get(t, 0));
            JavaRDD<Tuple2<String, String>> BubbleCanon = BubbleSingle.union(BubbleDouble);

            // File FASTA di output
            BubbleCanon.saveAsTextFile("data\\outputTXT");
        }
}
