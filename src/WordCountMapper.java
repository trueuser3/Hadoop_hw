import java.io.IOException;

public class WordCountMapper {
    public void map(String line, Emitter emitter) throws IOException {
        String[] words = line.split("\\s+");
        for (String word : words) {
            if (!word.isEmpty()) {
                emitter.emit(word, "1");
            }
        }
    }
}
