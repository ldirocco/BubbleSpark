import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import shapeless.Tuple;

import java.nio.charset.StandardCharsets;

public class getCanonicalBubble implements PairFunction<Tuple2<String,String>, Tuple2<String, String>, Tuple2<String,String>> {

    private Integer k;

    public getCanonicalBubble(Integer k) {
        this.k = k;
    }

    @Override
    public Tuple2<Tuple2<String, String>, Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
        byte[] kmer1 = tuple._1.getBytes(StandardCharsets.UTF_8);
        byte[] kmer2 = tuple._2.getBytes(StandardCharsets.UTF_8);

        byte[] kmer_rc1 = new byte[kmer1.length];
        byte[] kmer_rc2 = new byte[kmer2.length];

        for (int i = 0; i < kmer1.length; i++)
            switch (kmer1[kmer1.length - 1 - i]) {
                case 'A': kmer_rc1[i] = 'T'; break;
                case 'C': kmer_rc1[i] = 'G'; break;
                case 'G': kmer_rc1[i] = 'C'; break;
                case 'T': kmer_rc1[i] = 'A'; break;

                case 'a': kmer_rc1[i] = 't'; break;
                case 'c': kmer_rc1[i] = 'g'; break;
                case 'g': kmer_rc1[i] = 'c'; break;
                case 't': kmer_rc1[i] = 'a'; break;
            }

        for (int i = 0; i < kmer2.length; i++)
            switch (kmer2[kmer2.length - 1 - i]) {
                case 'A': kmer_rc2[i] = 'T'; break;
                case 'C': kmer_rc2[i] = 'G'; break;
                case 'G': kmer_rc2[i] = 'C'; break;
                case 'T': kmer_rc2[i] = 'A'; break;

                case 'a': kmer_rc2[i] = 't'; break;
                case 'c': kmer_rc2[i] = 'g'; break;
                case 'g': kmer_rc2[i] = 'c'; break;
                case 't': kmer_rc2[i] = 'a'; break;
            }

        String K_rc1 = new String(kmer_rc1);
        String K_rc2 = new String(kmer_rc2);
        String K_1 = new String(kmer1);
        String K_2 = new String(kmer2);
        if(K_rc1.compareToIgnoreCase(K_1)<0){
            K_1 = new String(K_rc1);
        }
        if(K_rc2.compareToIgnoreCase(K_2)<0){
            K_2 = new String(K_rc2);
        }
        Tuple2<String, String> res = tuple;
        if(!K_1.equals(tuple._1)){
            res = new Tuple2<>(K_2, K_1);
        }
        return new Tuple2<>(res, tuple);
    }
}
