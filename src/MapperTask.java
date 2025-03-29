import java.io.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class MapperTask implements Runnable {
    private final File inputFile;
    private final File outputDir;
    private final int numReducers;
    private final String mapperClassName;

    public MapperTask(File inputFile, File outputDir, int numReducers, String mapperClassName) {
        this.inputFile = inputFile;
        this.outputDir = outputDir;
        this.numReducers = numReducers;
        this.mapperClassName = mapperClassName;
        outputDir.mkdirs();
    }

    @Override
    public void run() {
        try {
            Class<?> mapperClass = Class.forName(mapperClassName);
            Object mapper = mapperClass.getDeclaredConstructor().newInstance();
            Method mapMethod = mapperClass.getMethod("map", String.class, Emitter.class);

            List<List<String>> partitions = new ArrayList<>(numReducers);
            for (int i = 0; i < numReducers; i++) {
                partitions.add(new ArrayList<>());
            }

            Emitter emitter = (key, value) -> {
                int partition = (key.hashCode() & Integer.MAX_VALUE) % numReducers;
                partitions.get(partition).add(key + "\t" + value);
            };

            try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    mapMethod.invoke(mapper, line, emitter);
                }
            }

            for (int i = 0; i < numReducers; i++) {
                File partFile = new File(outputDir, "part-" + i);
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(partFile))) {
                    for (String entry : partitions.get(i)) {
                        writer.write(entry);
                        writer.newLine();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
