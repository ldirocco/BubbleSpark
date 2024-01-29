import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

public class BinsExtractorFromPartition implements Serializable, FlatMapFunction<Iterator<Tuple2<Integer,byte[]>>,Tuple2<Integer,Hashtable<Long, Long>>> {

    private int k;
    private int nBin;
    public BinsExtractorFromPartition(int k,int nBin){
        this.k=k;
        this.nBin=nBin;
    }

    @Override
    public Iterator<Tuple2<Integer,Hashtable<Long, Long>>> call(Iterator<Tuple2<Integer, byte[]>> iter) throws Exception {
        Tuple2<Integer,byte[]> read;
        Long kmer;
        int len;
        Hashtable<Long, Long>[] frequencies = new Hashtable[nBin];
        KmerTool ktool;
        long index;


        while (iter.hasNext()){
            read=iter.next();
            len=read._2.length;
            ktool = new KmerTool(read._2, k);
            for (int i = 0; i < len - (k-1); i++) {
                kmer = ktool.nextKmerCan();
                int idBin = Math.abs(Long.hashCode(kmer) % nBin);
                if (frequencies[idBin] == null)
                    frequencies[idBin] = new Hashtable<>();
                index=1l;
                if(read._1==1){
                    index=index<<32;
                }
                frequencies[idBin].merge(kmer, index, Long::sum);
            }
        }

        List<Tuple2<Integer, Hashtable<Long, Long>>> kmers = new ArrayList<>();
        for (int idBin = 0; idBin < nBin; idBin++)
            if (frequencies[idBin] != null)
                kmers.add(new Tuple2<>(idBin,frequencies[idBin]));

        return kmers.iterator();
    }
}

