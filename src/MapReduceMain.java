import java.io.*;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MapReduceMain {
    public static void main(String[] args) throws Exception {
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

        File tempDir = Files.createTempDirectory("mapreduce").toFile();
        List<Thread> mappers = new ArrayList<>();
        AtomicInteger mapperId = new AtomicInteger();

        for (File file : inputFiles) {
            Thread mapperThread = new Thread(new MapperTask(
                    file,
                    new File(tempDir, "mapper-" + mapperId.getAndIncrement()),
                    numReducers,
                    mapperClassName
            ));
            mappers.add(mapperThread);
            mapperThread.start();
        }

        for (Thread mapper : mappers) {
            mapper.join();
        }

        List<Thread> reducers = new ArrayList<>();
        for (int i = 0; i < numReducers; i++) {
            Thread reducerThread = new Thread(new ReducerTask(
                    tempDir,
                    new File(outputDir),
                    i,
                    numReducers,
                    reducerClassName
            ));
            reducers.add(reducerThread);
            reducerThread.start();
        }

        for (Thread reducer : reducers) {
            reducer.join();
        }

        deleteDirectory(tempDir);
    }

    private static void deleteDirectory(File dir) throws IOException {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        Files.delete(dir.toPath());
    }
}
