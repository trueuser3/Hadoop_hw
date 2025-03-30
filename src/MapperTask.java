import java.io.*;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

class MapperTask implements Runnable {
    private final File inputFile;
    private final File outputDir;
    private final int numReducers;
    private final String mapperClassName;
    private final int bufferSize;
    
    public MapperTask(File inputFile, File outputDir, int numReducers, 
                     String mapperClassName, int bufferSize) {
        this.inputFile = inputFile;
        this.outputDir = outputDir;
        this.numReducers = numReducers;
        this.mapperClassName = mapperClassName;
        this.bufferSize = bufferSize;
        outputDir.mkdirs();
    }

    @Override
    public void run() {
        System.out.println("Processing file: " + inputFile.getName());
        try {
            Class<?> mapperClass = Class.forName(mapperClassName);
            Object mapper = mapperClass.getDeclaredConstructor().newInstance();
            Method mapMethod = mapperClass.getMethod("map", String.class, Emitter.class);

            Map<Integer, BufferedWriter> writers = new HashMap<>();
            Map<Integer, Integer> buffers = new HashMap<>();

            Emitter emitter = (key, value) -> {
                int partition = Math.abs(key.hashCode()) % numReducers;
                
                BufferedWriter writer = writers.computeIfAbsent(partition, p -> {
                    try {
                        return new BufferedWriter(new FileWriter(
                            new File(outputDir, "part-" + p), true));
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to create writer", e);
                    }
                });
                
                try {
                    writer.write(key + "\t" + value + "\n");
                } catch (IOException e) {
                    throw new RuntimeException("Write error", e);
                }
                
                int count = buffers.getOrDefault(partition, 0) + 1;
                if (count >= bufferSize) {
                    try {
                        writer.flush();
                    } catch (IOException e) {
                        throw new RuntimeException("Flush error", e);
                    }
                    buffers.put(partition, 0);
                } else {
                    buffers.put(partition, count);
                }
            };

            int lineCount = 0;
            try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    mapMethod.invoke(mapper, line, emitter);
                    lineCount++;
                }
            }

            for (BufferedWriter writer : writers.values()) {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            // Создание пустых файлов для всех партиций
            for (int p = 0; p < numReducers; p++) {
                File partFile = new File(outputDir, "part-" + p);
                if (!partFile.exists()) {
                    try {
                        partFile.createNewFile();
                        System.out.println("Created empty partition: " + partFile.getName());
                    } catch (IOException e) {
                        System.err.println("Failed to create empty partition: " + partFile);
                        e.printStackTrace();
                    }
                }
            }
            
            File completeFile = new File(outputDir, ".complete");
            if (!completeFile.createNewFile()) {
                throw new IOException("Failed to create completion marker");
            }
            System.out.println("Mapper completed: " + inputFile.getName());

        } catch (Exception e) {
            System.err.println("Mapper failed for: " + inputFile.getAbsolutePath());
            e.printStackTrace();
            try {
                new File(outputDir, ".error").createNewFile();
            } catch (IOException ex) {
                System.err.println("Failed to create error marker");
            }
        }
    }
}
