
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Tema2 {

    public static ArrayList<Integer> fileInputs = new ArrayList<>();
    public static String delimiters = ";:/?˜\\.,><‘[]{}()!@#$%ˆ&- +'=*\"\n\r\t";

    // this is the central method, which will create workers for map and reduce, will run the program and do all the
    // small tricks, like interpreting the input and generating the output
    public static void start(Integer nrWorkers, String testIn, String testOut) throws ExecutionException, InterruptedException {
        Integer fragmentSize = null;
        Integer nrFiles = null;
        // create a list with the names of the files
        List<String> fileNames = Collections.synchronizedList(new ArrayList<>());
        try {
            File myObj = new File(testIn);
            Scanner myReader = new Scanner(myObj);
            fragmentSize = Integer.valueOf(myReader.nextLine());
            nrFiles = Integer.valueOf(myReader.nextLine());
            for(int i = 0; i < nrFiles; i++) {
                fileNames.add(myReader.nextLine());
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (nrFiles == null) {
            System.err.println("number of files not initialized");
            return;
        }
        /* MAP */
        // create the thread pool with the given number of workers
        // this executor will be used for map operation and also for reduce
        ExecutorService tpe = Executors.newFixedThreadPool(nrWorkers);
        List<CompletableFuture<HashMap<Integer, Integer>>> CFhashMapsList = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);
        // iterate through files
        for (int i = 0; i < nrFiles; i++) {
            Tema2.fileInputs.add((int) new File(fileNames.get(i)).length());
            // "break" every input file in fragments of fragmentSize size
            for (int j = 0; j < Tema2.fileInputs.get(i); j += fragmentSize) {
                counter.incrementAndGet();
                CompletableFuture<HashMap<Integer, Integer>> CFhashMap = new CompletableFuture<>();
                CFhashMapsList.add(CFhashMap);
                // append the tasks
                tpe.submit(new Map(tpe, counter, fileNames.get(i), j, fragmentSize, i, CFhashMap));
            }
        }
        // await termination of all tasks
        try {
            tpe.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // the results obtained after map operation are stored in a list of completable Future; we get what we need
        // from this
        /* REDUCE */
        for (CompletableFuture<HashMap<Integer, Integer>> hashMapCompletableFuture : CFhashMapsList) {
            if (hashMapCompletableFuture.toString().contains("Not")) {
                hashMapCompletableFuture.complete(new HashMap<>());
            }
        }

        // use the same executor for reduce operation
        tpe = Executors.newFixedThreadPool(nrWorkers);
        List<CompletableFuture<String>> output = new ArrayList<>();
        counter.set(0);
        // iterate through files
        for (int i = 0; i < nrFiles; i++) {
            List<HashMap<Integer, Integer>> seqHash = new ArrayList<>();
            // iterate through the hash maps obtained from Map operation
            for (int j = 0; j < Tema2.fileInputs.get(i); j += fragmentSize) {
                try {
                    if (CFhashMapsList.size() == 0) {
                        continue;
                    }
                    seqHash.add(CFhashMapsList.get(0).get());
                    CFhashMapsList.remove(0);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            // create new tasks, this time for Reduce
            counter.incrementAndGet();
            CompletableFuture<String> fileResult = new CompletableFuture<>();
            output.add(fileResult);
            tpe.submit(new Reduce(tpe, counter,
                            fileNames.get(i).split("/")[fileNames.get(i).split("/").length - 1],
                            seqHash, fileResult));
        }
        // await termination of all tasks
        try {
            tpe.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // get from completable future the results for every input file
        List<String> finalResults = new ArrayList<>();
        for (CompletableFuture<String> x : output) {
            finalResults.add(x.get());
        }
        // sort the results after the ranking; the compare function is using integers
        finalResults.sort( (String s1, String s2) -> (int)(Float.parseFloat(s2.split(",")[1]) * 100) -
                (int)(Float.parseFloat(s1.split(",")[1]) * 100));
        // write the final result to the output file
        try {
            FileWriter myWriter = new FileWriter(testOut);
            for (String x : finalResults) {
                myWriter.write(x);
                myWriter.write('\n');
            }
            myWriter.close();
            myWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    // return the n-th fibonacci number
    public static long Fibonacci(int termNumber)
    {
        return (termNumber == 1) ? 0 : (termNumber == 2) ? 1 : Fibonacci(termNumber - 1) + Fibonacci(termNumber -2);
    }

    // main function -- get the arguments from command line
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }
        Integer nrWorkers = Integer.valueOf(args[0]);
        String testIn = args[1];
        String testOut = args[2];
        Tema2.start(nrWorkers, testIn, testOut);

    }
}
