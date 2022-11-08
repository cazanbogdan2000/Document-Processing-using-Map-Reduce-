import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class Reduce implements Runnable {
    ExecutorService tpe;
    AtomicInteger counter;
    String fileName;
    List<HashMap<Integer, Integer>> seqHash;
    CompletableFuture<String> fileResult;

    // constructor for a task of type Map
    public Reduce(ExecutorService tpe, AtomicInteger counter, String fileName,
                  List<HashMap<Integer, Integer>> seqHash, CompletableFuture<String> fileResult) {
        this.tpe = tpe;
        this.counter = counter;
        this.fileName = fileName;
        this.seqHash = seqHash;
        this.fileResult = fileResult;
    }

    @Override
    public void run() {
        HashMap<Integer, Integer> resultedHash = new HashMap<>();
        // unify all the hash maps obtained at reduce in a bigger one, which represents the frequency for the entire
        // file
        for (var y : seqHash) {
            for (var x : y.keySet()) {
                if (resultedHash.containsKey(x)) {
                    resultedHash.put(x, resultedHash.get(x) + y.get(x));
                }
                else {
                    resultedHash.put(x, y.get(x));
                }
            }
        }
        float rank;
        long nrWords = 0;
        long fibbFormula = 0;
        int maxKey = 0, maxValue = 0;
        // compute the rank using the formula given in the homework
        for (var x : resultedHash.keySet()) {
            if (x > maxKey) {
                maxKey = x;
                maxValue = resultedHash.get(x);
            }
            fibbFormula += Tema2.Fibonacci(x + 2) * resultedHash.get(x);
            nrWords += resultedHash.get(x);
        }
        // converting the result to string, and after that, returning it
        rank = ((float)fibbFormula) / nrWords;
        String result = fileName + "," + String.format("%.2f", rank) + "," + maxKey + "," + maxValue;
        fileResult.complete(result);
        // shut down the thread pool if there are no remaining tasks
        int left = counter.decrementAndGet();
        if (left == 0) {
            tpe.shutdown();
        }
    }
}
