import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

public class MainBeta {
    public static void main(String[] args) {
        Long2IntOpenHashMap[] frequencies = new Long2IntOpenHashMap[3];
        int index=0;
        if (frequencies[index] == null)
            frequencies[index] = new Long2IntOpenHashMap();
        frequencies[index].merge(2l, 1, Integer::sum);
        System.out.println(frequencies[0].size());
    }
}
