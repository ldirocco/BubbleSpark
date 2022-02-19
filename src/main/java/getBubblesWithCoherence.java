import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;

public class getBubblesWithCoherence implements PairFunction<Tuple2<Integer, Iterable<Tuple2<String, Tuple2<Integer, Integer>>>>, Tuple3<Integer,String, String>, Tuple2<Integer, Integer>> {

    @Override
    public Tuple2<Tuple3<Integer, String, String>, Tuple2<Integer, Integer>> call(Tuple2<Integer, Iterable<Tuple2<String, Tuple2<Integer, Integer>>>> t) throws Exception {
        ArrayList<Tuple2<String, Tuple2<Integer, Integer>>> array = new ArrayList<>();
        t._2.forEach(array::add);
        return new Tuple2<>(new Tuple3<>(t._1, array.get(0)._1, array.get(1)._1), new Tuple2<>(Integer.max(array.get(0)._2._1, array.get(0)._2._2), Integer.max(array.get(1)._2._1, array.get(1)._2._2)));
    }
}
