import java.io.IOException;
import java.util.List;

public class WordCountReducer {
    public void reduce(String key, List<String> values, Emitter emitter) throws IOException {
        int sum = 0;
        for (String value : values) {
            sum += Integer.parseInt(value);
        }
        emitter.emit(key, String.valueOf(sum));
    }
}
