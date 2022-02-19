import org.apache.spark.api.java.function.Function;
import scala.Tuple3;

import java.util.ArrayList;

public class getOutput implements Function<Tuple3<Integer, String, String>, String> {

    @Override
    public String call(Tuple3<Integer, String, String> v) throws Exception {
        String s = "("+v._2()+","+v._3()+")";
        return s;
    }
}
