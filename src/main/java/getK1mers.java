import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;

public class getK1mers implements Function<String, ArrayList<String>> {
    private Integer k;

    public getK1mers(Integer k1) {
        this.k = k1;
    }

    @Override
    public ArrayList<String> call(String v1) throws Exception {
        ArrayList<String> kmers = new ArrayList<String>();
        Integer num = v1.length();
        for (int i = 0; i < num-k+1; i++){
            //String kmer = new String(KmerUtils.getCanonical(v1.substring(i, k+i).getBytes()));
            kmers.add(v1.substring(i, k+i));
        }
        return kmers;
    }
}
