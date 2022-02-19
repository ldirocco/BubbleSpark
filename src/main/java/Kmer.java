import scala.Tuple2;

import java.io.Serializable;

public class Kmer implements Serializable {
    public final long kmer;
    public final int k;
    //public final boolean b;

    public Kmer(byte[] k2byte) {
        kmer=encode(k2byte);
        k=k2byte.length;
        //b=getCanonical(k2byte)._2;
    }

    public int hashCode() {
        return Long.hashCode(kmer);
    }

    public boolean filter(String regex) {
        return new String(decode()).matches(regex);
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof Kmer)) return false;

        return ((Kmer)obj).kmer == kmer;
    }
    public String toString() {
        return new String(decode());
    }
/*
    public String toStringOriginal() {
        return new String(getOriginal());
    }
/*
    private byte[] getOriginal() {
        byte[] kmer = decode();
        byte[] kmer_rc = new byte[kmer.length];
        if(this.b) {
            for (int i = 0; i < kmer.length; i++)
                switch (kmer[kmer.length - 1 - i]) {
                    case 'A': kmer_rc[i] = 'T'; break;
                    case 'C': kmer_rc[i] = 'G'; break;
                    case 'G': kmer_rc[i] = 'C'; break;
                    case 'T': kmer_rc[i] = 'A'; break;

                    case 'a': kmer_rc[i] = 't'; break;
                    case 'c': kmer_rc[i] = 'g'; break;
                    case 'g': kmer_rc[i] = 'c'; break;
                    case 't': kmer_rc[i] = 'a'; break;
                }
            return kmer_rc;
        } else {
            return kmer;
        }
    }
*/

    public static Tuple2<byte[], Boolean> getCanonical(byte[] kmer) {
        byte[] kmer_rc = new byte[kmer.length];

        for (int i = 0; i < kmer.length; i++)
            switch (kmer[kmer.length - 1 - i]) {
                case 'A': kmer_rc[i] = 'T'; break;
                case 'C': kmer_rc[i] = 'G'; break;
                case 'G': kmer_rc[i] = 'C'; break;
                case 'T': kmer_rc[i] = 'A'; break;

                case 'a': kmer_rc[i] = 't'; break;
                case 'c': kmer_rc[i] = 'g'; break;
                case 'g': kmer_rc[i] = 'c'; break;
                case 't': kmer_rc[i] = 'a'; break;
            }

        Tuple2<byte[], Boolean> KmerB = new Tuple2<>(kmer, false);;
        for (int i = 0; i < kmer.length; i++) {
            if (kmer[i] < kmer_rc[i])
                KmerB = new Tuple2<>(kmer, false);
            else if (kmer_rc[i] < kmer[i])
                KmerB = new Tuple2<>(kmer_rc, true);
        }

        return KmerB;
    }

    private long encode(byte[] kmer) {
        long kmer_enc = 0;

        for (byte b : kmer) {
            kmer_enc <<= 2;
            switch (b) {
                case 'A':
                case 'a':
                    kmer_enc += 0; break;
                case 'C':
                case 'c':
                    kmer_enc += 1; break;
                case 'G':
                case 'g':
                    kmer_enc += 2; break;
                case 'T':
                case 't':
                    kmer_enc += 3; break;
            }
        }

        return kmer_enc;
    }

    public byte[] decode() {
        byte[] kmer_dec = new byte[k];
        long kmer = this.kmer;

        for (int i = k-1; i >= 0; i--) {
            byte b = (byte) (kmer & 0b11);
            kmer >>= 2;

            switch (b) {
                case 0b00:
                    kmer_dec[i] = 'A'; break;
                case 0b01:
                    kmer_dec[i] = 'C'; break;
                case 0b10:
                    kmer_dec[i] = 'G'; break;
                case 0b11:
                    kmer_dec[i] = 'T'; break;
            }
        }

        return kmer_dec;
    }
}
