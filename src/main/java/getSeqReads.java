import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

public class getSeqReads implements Function<String, ArrayList<SeqRead>> {

    private List<Tuple2<String, String>> seq;
    private Integer k;

    public getSeqReads(Broadcast<Integer> k0, Broadcast<List<Tuple2<String, String>>> sequences) {
        this.seq = sequences.value();
        this.k = k0.value();
    }

    @Override
    public ArrayList<SeqRead> call(String s) throws Exception {
        ArrayList<SeqRead> kmers = new ArrayList<SeqRead>();
        int q=0;
        for (Tuple2<String, String> t : seq) {
            Integer num = t._1.length();
            q=q+1;
            for (int i = 0; i < num-k+1; i++){
                if(s.contains(t._1.substring(i, k+i))) {
                    //kmers.add(new Tuple2<>(t._1, t._1.substring(i, k+i)));
                    //SeqRead e = new SeqRead(q,t._1,(i+1));
                    //new SeqRead(q, t._1, i+1);
                    kmers.add(new SeqRead(q, t._1, (i+1)));
                    //kmers.add(new Tuple3<>(q, t._1, i+1));
                }
                if(s.contains(t._2.substring(i,k+i))) {
                    //kmers.add(new Tuple2<>(t._2, t._2.substring(i, k+i)));
                    kmers.add(new SeqRead(q, t._2, (i+1)));
                }
            }

        }
        return kmers;
    }
}
