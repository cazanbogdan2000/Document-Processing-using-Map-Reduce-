import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class Map implements Runnable {
    ExecutorService tpe;
    AtomicInteger counter;
    String fileName;
    int offset;
    int fragmentSize;
    int indexOfFile;
    CompletableFuture<HashMap<Integer, Integer>> CFhashMap;

    // constructor for a task of type Map
    public Map(ExecutorService tpe, AtomicInteger counter, String fileName, int offset, int fragmentSize,
               int indexOfFile, CompletableFuture<HashMap<Integer, Integer>> CFhashMap) {
        this.tpe = tpe;
        this.counter = counter;
        this.fileName = fileName;
        this.offset = offset;
        this.fragmentSize = fragmentSize;
        this.indexOfFile = indexOfFile;
        this.CFhashMap = CFhashMap;
    }

    @Override
    public void run() {
        RandomAccessFile file;
        // read a partition of a file, from a starting point to the ending point (depending on the fragment Size)
        try {
            file = new RandomAccessFile(fileName, "r");
            file.seek(offset);
            byte[] bytes = new byte[fragmentSize];
            List<Character> sequence = Collections.synchronizedList(new ArrayList<>());
            file.read(bytes);
            for (int i = 0; i < fragmentSize; i++) {
                sequence.add((char)bytes[i]);
            }
            // see if the precedent character from the sequence is a delimiter or not; if not; pop the broken word from
            // sequence
            if (offset != 0) {
                file.seek(offset - 1);
                byte[] b = new byte[1];
                file.read(b);
                // there is something different from a delimiter
                if(Tema2.delimiters.indexOf((char)b[0]) == -1) {
                    while (sequence.size() != 0 && Tema2.delimiters.indexOf(sequence.get(0)) == -1) {
                        sequence.remove(0);
                    }
                }
            }
            // check if we reach the end of the file, and we have exhausted all the files
            if (sequence.size() == 0) {
                int left = counter.decrementAndGet();
                if (left == 0) {
                    CFhashMap.complete(null);
                    tpe.shutdown();
                }
                return;
            }
            // if the end of the sequence is not a delimiter, we have to read the rest of the word
            if (Tema2.delimiters.indexOf(sequence.get(sequence.size() - 1)) == -1) {
                offset += fragmentSize;
                byte[] b = new byte[1];
                while (offset < Tema2.fileInputs.get(indexOfFile)) {
                    file.seek(offset);
                    file.read(b);
                    if (Tema2.delimiters.indexOf((char)b[0]) == -1 && b[0] != 0) {
                        sequence.add((char)b[0]);
                        offset++;
                    }
                    else {
                        break;
                    }
                }
            }
            // if the sequence starts with a piece of word, then we will exclude that word
            int k = 0;
            while (k < sequence.size()) {
                char c = sequence.get(k);
                byte b = (byte) c;
                if (b == 0) {
                    sequence.remove(k);
                }
                else {
                    k++;
                }
            }
            // create the hashmap for that fragment of input
            HashMap<Integer, Integer> frequency = new HashMap<>();
            // using string tokenizer, in order to split our sequence in words
            String seqStr = sequence.toString().substring(1, 3 * sequence.size() - 1).replaceAll(", ", "");
            StringTokenizer defaultTokenizer = new StringTokenizer(seqStr, Tema2.delimiters);
            while (defaultTokenizer.hasMoreTokens()) {
                String nextToken = defaultTokenizer.nextToken();
                if (frequency.containsKey(nextToken.length())) {
                    frequency.replace(nextToken.length(), frequency.get(nextToken.length()) + 1);
                }
                else {
                    frequency.put(nextToken.length(), 1);
                }
            }
            // actualize the completable future
            CFhashMap.complete(frequency);
            // shut down the thread pool if there are no remaining tasks
            int left = counter.decrementAndGet();
            if (left == 0) {
                tpe.shutdown();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
