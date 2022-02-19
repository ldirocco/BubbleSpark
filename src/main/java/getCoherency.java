import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Array;
import scala.Tuple2;
import shapeless.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;


public class getCoherency implements PairFunction<Tuple2<Tuple2<Integer,String>, Iterable<Tuple2<Integer, Integer>>>, Tuple2<Integer,String>, Integer> {

    private Integer k;

    public getCoherency(Broadcast<Integer> k0) {
        this.k = k0.value();
    }


    @Override
    public Tuple2<Tuple2<Integer,String>, Integer> call(Tuple2<Tuple2<Integer,String>, Iterable<scala.Tuple2<Integer, Integer>>> s) throws Exception {
        Integer q=0;
        ArrayList<Tuple2<Integer, Integer>> array = new ArrayList<>();
        s._2.forEach(array::add);
        //array.sort((t,v)-> t._1.compareTo(v._1));
        int n = array.toArray().length;
        if (n==k+2) {
            q=array.get(0)._2;
            for(Tuple2<Integer, Integer> t : array) {
                if(t._2<q) {
                    q=t._2;
                }
            }
        }
        return new scala.Tuple2<>(s._1, q);
    }
}
