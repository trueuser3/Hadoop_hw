import java.io.*;
import java.lang.reflect.Method;
import java.nio.file.*;
import java.util.*;

public class MapReduceMain {
    private static final int BUFFER_SIZE = 1000;

    public static void main(String[] args) throws Exception {
        System.out.println("Starting MapReduce...");
        
        if (args.length < 5) {
            System.err.println("Usage: MapReduceMain <inputDir> <outputDir> <numReducers> <MapperClass> <ReducerClass>");
            System.exit(1);
        }

        String inputDir = args[0];
        String outputDir = args[1];
        int numReducers = Integer.parseInt(args[2]);
        String mapperClassName = args[3];
        String reducerClassName = args[4];

        File inputDirFile = new File(inputDir);
        File[] inputFiles = inputDirFile.listFiles(File::isFile);
        if (inputFiles == null || inputFiles.length == 0) {
            throw new IllegalArgumentException("Input directory contains no files");
        }

        System.out.println("Found " + inputFiles.length + " input files");
        File tempDir = Files.createTempDirectory("mapreduce").toFile();
        System.out.println("Temp dir: " + tempDir.getAbsolutePath());

        List<Thread> mappers = new ArrayList<>();
        int mapperId = 0;

        for (File file : inputFiles) {
            System.out.println("Starting mapper for: " + file.getName());
            Thread mapperThread = new Thread(new MapperTask(
                    file,
                    new File(tempDir, "mapper-" + mapperId++),
                    numReducers,
                    mapperClassName,
                    BUFFER_SIZE
            ));
            mappers.add(mapperThread);
            mapperThread.start();
        }

        List<Thread> reducers = new ArrayList<>();
        System.out.println("Starting " + numReducers + " reducers");
        for (int i = 0; i < numReducers; i++) {
            Thread reducerThread = new Thread(new ReducerTask(
                    tempDir,
                    new File(outputDir),
                    i,
                    numReducers,
                    reducerClassName,
                    inputFiles.length
            ));
            reducers.add(reducerThread);
            reducerThread.start();
        }

        Thread shutdownHook = new Thread(() -> {
            System.err.println("\nEmergency shutdown!");
            printThreadStackTraces();
            try {
                deleteDirectory(tempDir);
            } catch (IOException e) {
                System.err.println("Failed to clean temp dir: " + e.getMessage());
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        System.out.println("Waiting for mappers...");
        for (Thread mapper : mappers) mapper.join();
        System.out.println("All mappers completed");
        
        System.out.println("Waiting for reducers...");
        for (Thread reducer : reducers) reducer.join();
        System.out.println("All reducers completed");

        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        deleteDirectory(tempDir);
    }

    private static void deleteDirectory(File dir) throws IOException {
        Files.walk(dir.toPath())
             .sorted(Comparator.reverseOrder())
             .forEach(path -> {
                 try { Files.delete(path); } 
                 catch (IOException e) { /* Ignore */ }
             });
    }

    private static void printThreadStackTraces() {
        Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();
        for (Map.Entry<Thread, StackTraceElement[]> entry : stacks.entrySet()) {
            System.err.println("Thread: " + entry.getKey().getName());
            for (StackTraceElement element : entry.getValue()) {
                System.err.println("    " + element);
            }
        }
    }
}
