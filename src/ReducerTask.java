import java.io.*;
import java.lang.reflect.Method;
import java.util.*;

class ReducerTask implements Runnable {
    private final File tempDir;
    private final File outputDir;
    private final int partition;
    private final int numReducers;
    private final String reducerClassName;
    private final int totalMappers;

    public ReducerTask(File tempDir, File outputDir, int partition, 
                      int numReducers, String reducerClassName, int totalMappers) {
        this.tempDir = tempDir;
        this.outputDir = outputDir;
        this.partition = partition;
        this.numReducers = numReducers;
        this.reducerClassName = reducerClassName;
        this.totalMappers = totalMappers;
        outputDir.mkdirs();
    }

    @Override
    public void run() {
        System.out.println("Starting reducer #" + partition);
        try {
            Class<?> reducerClass = Class.forName(reducerClassName);
            Object reducer = reducerClass.getDeclaredConstructor().newInstance();
            Method reduceMethod = reducerClass.getMethod("reduce", String.class, List.class, Emitter.class);

            Map<String, List<String>> grouped = new HashMap<>();
            File outFile = new File(outputDir, "part-" + partition);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
                Emitter emitter = (key, value) -> {
                    try {
                        writer.write(key + "\t" + value + "\n");
                    } catch (IOException e) {
                        throw new RuntimeException("Reducer write error", e);
                    }
                };

                int completedMappers = 0;
                int retryCount = 0;
                Set<File> processedMappers = new HashSet<>();
                final int MAX_RETRIES = totalMappers * 5;

                while (completedMappers < totalMappers && retryCount < MAX_RETRIES) {
                    File[] mapperDirs = tempDir.listFiles((dir, name) -> name.startsWith("mapper-"));
                    if (mapperDirs == null) {
                        retryCount++;
                        Thread.sleep(500);
                        continue;
                    }

                    for (File mapperDir : mapperDirs) {
                        if (processedMappers.contains(mapperDir)) continue;

                        File completeFile = new File(mapperDir, ".complete");
                        if (!completeFile.exists()) continue;

                        File partFile = new File(mapperDir, "part-" + partition);
                        
                        // Обработка отсутствующих файлов партиций
                        if (!partFile.exists()) {
                            System.out.printf("Reducer %d: no data in %s%n", 
                                partition, mapperDir.getName());
                            processedMappers.add(mapperDir);
                            completedMappers++;
                            continue;
                        }

                        try (BufferedReader reader = new BufferedReader(new FileReader(partFile))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                String[] parts = line.split("\t", 2);
                                if (parts.length != 2) continue;
                                grouped.computeIfAbsent(parts[0], k -> new ArrayList<>()).add(parts[1]);
                            }
                        }
                        partFile.delete();
                        processedMappers.add(mapperDir);
                        completedMappers++;
                        System.out.printf("Reducer %d: processed %d/%d mappers%n", 
                            partition, completedMappers, totalMappers);
                    }
                    
                    if (completedMappers < totalMappers) {
                        retryCount++;
                        Thread.sleep(1000);
                        System.out.printf("Reducer %d: waiting... retry %d/%d%n",
                            partition, retryCount, MAX_RETRIES);
                    }
                }

                if (retryCount >= MAX_RETRIES) {
                    System.err.printf("Reducer %d TIMEOUT! Completed %d/%d mappers%n",
                        partition, completedMappers, totalMappers);
                }

                List<String> keys = new ArrayList<>(grouped.keySet());
                Collections.sort(keys);
                System.out.printf("Reducer %d: processing %d keys%n", partition, keys.size());
                
                for (String key : keys) {
                    reduceMethod.invoke(reducer, key, grouped.get(key), emitter);
                }
            }
            System.out.println("Reducer #" + partition + " completed");

        } catch (Exception e) {
            System.err.println("Reducer #" + partition + " failed:");
            e.printStackTrace();
        }
    }
}
