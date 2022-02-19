import scala.Tuple3;

import java.io.Serializable;

public class SeqRead implements Serializable {
    public final int BubbleID;
    public final String Seq;
    public final int KmerPos;

    public SeqRead(int bubbleID, String seq, int kmerPos) {
        this.BubbleID = bubbleID;
        this.Seq = seq;
        this.KmerPos = kmerPos;
    }

    public int hashCode() { return Integer.hashCode(BubbleID); }

    public boolean equals(Object obj) {
        if(!(obj instanceof SeqRead)) return false;

        return ((SeqRead)obj).BubbleID==BubbleID & ((SeqRead)obj).Seq.equals(Seq) & ((SeqRead)obj).KmerPos==KmerPos;
    }
}
