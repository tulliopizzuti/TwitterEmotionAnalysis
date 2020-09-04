import org.chaiware.app.TextToEmotion;
import org.chaiware.emotion.EmotionalState;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader("39.json"));
        String line = reader.readLine();

        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
        hashMap.put("NEUTRAL", 0);
        hashMap.put("HAPPINESS", 0);
        hashMap.put("SADNESS", 0);
        hashMap.put("FEAR", 0);
        hashMap.put("ANGER", 0);
        hashMap.put("DISGUST", 0);
        hashMap.put("SURPRISE", 0);

        while (line != null) {
            if(!(line.substring(2, 8).equals("delete"))){
                JSONObject obj = new JSONObject(line);
                String lang = obj.getString("lang");
                String text = obj.getString("text");

                if(lang.equals("en")) {
                    try {
                        EmotionalState e = TextToEmotion.textToEmotion(text);
                        Integer cc = hashMap.get(e.getStrongestEmotionAsString());
                        cc++;
                        hashMap.put(e.getStrongestEmotionAsString(), cc);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
            line = reader.readLine();
        }
        reader.close();

        Iterator iterator = hashMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry me2 = (Map.Entry) iterator.next();
            System.out.println(me2.getKey() + " - " + me2.getValue());
        }
    }
}