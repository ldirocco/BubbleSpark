public class KmerUtils {
    public static byte[] getCanonical(byte[] kmer) {
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

        for (int i = 0; i < kmer.length; i++) {
            if (kmer[i] < kmer_rc[i])
                return kmer;
            else if (kmer_rc[i] < kmer[i])
                return kmer_rc;
        }

        return kmer;
    }

    public static byte[] decode(Kmer k) {
        byte[] kmer_dec = new byte[k.k];
        long kmer = k.kmer;

        for (int i = k.k-1; i >= 0; i--) {
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

    public static long encode(byte[] kmer) {
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

}
