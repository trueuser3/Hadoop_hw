import java.io.*;
import java.lang.reflect.Method;
import java.util.*;

public class ReducerTask implements Runnable {
    private final File tempDir;
    private final File outputDir;
    private final int partition;
    private final int numReducers;
    private final String reducerClassName;

    public ReducerTask(File tempDir, File outputDir, int partition, int numReducers, String reducerClassName) {
        this.tempDir = tempDir;
        this.outputDir = outputDir;
        this.partition = partition;
        this.numReducers = numReducers;
        this.reducerClassName = reducerClassName;
        outputDir.mkdirs();
    }

    @Override
    public void run() {
        try {
            Class<?> reducerClass = Class.forName(reducerClassName);
            Object reducer = reducerClass.getDeclaredConstructor().newInstance();
            Method reduceMethod = reducerClass.getMethod("reduce", String.class, List.class, Emitter.class);

            Map<String, List<String>> grouped = new HashMap<>();
            File[] mapperDirs = tempDir.listFiles((dir, name) -> name.startsWith("mapper-"));
            if (mapperDirs == null) return;

            for (File mapperDir : mapperDirs) {
                File partFile = new File(mapperDir, "part-" + partition);
                if (!partFile.exists()) continue;

                try (BufferedReader reader = new BufferedReader(new FileReader(partFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t", 2);
                        if (parts.length != 2) continue;
                        grouped.computeIfAbsent(parts[0], k -> new ArrayList<>()).add(parts[1]);
                    }
                }
            }

            List<String> keys = new ArrayList<>(grouped.keySet());
            Collections.sort(keys);

            File outFile = new File(outputDir, "part-" + partition);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
                Emitter emitter = (key, value) -> {
                    writer.write(key + "\t" + value);
                    writer.newLine();
                };

                for (String key : keys) {
                    reduceMethod.invoke(reducer, key, grouped.get(key), emitter);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
