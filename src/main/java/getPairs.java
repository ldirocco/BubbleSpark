import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

public class getPairs implements Function<Tuple2<Kmer, Integer>, Tuple3<Kmer, Kmer, Integer>> {

    private Integer k;

    public getPairs(Integer k1) {
        this.k = k1;
    }

    @Override
    public Tuple3<Kmer, Kmer, Integer> call(Tuple2<Kmer, Integer> v1) throws Exception {
        Kmer v = new Kmer(v1._1.toString().substring(0,k-1).getBytes());
        Kmer u = new Kmer(v1._1.toString().substring(1,k).getBytes());
        Tuple3<Kmer, Kmer, Integer> t = new Tuple3<Kmer, Kmer, Integer>(v,u, v1._2);
        /*if (v1._1.b) {
            t = new Tuple3<Kmer, Kmer, Integer>(u,v, v1._2);
        }
        */
        return t;
    }
}
