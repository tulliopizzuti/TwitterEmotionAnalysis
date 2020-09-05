package it.unisa.degpizz.thread;

import org.chaiware.app.TextToEmotion;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MapCounter {
    private Map<String, Integer> map;

    public MapCounter() {
        map = new HashMap<>();
    }

    public synchronized void parseText(String text) throws Exception{
        increment(TextToEmotion.textToEmotionString(text));
    }

    public void increment(String key) {
        if (map.containsKey(key)) {
            Integer i = map.get(key);
            i++;
            map.put(key, i);
        } else {
            map.put(key, 1);

        }
    }

    public Map<String, Integer> getMap() {
        return map;
    }

    public void setMap(Map<String, Integer> map) {
        this.map = map;
    }

    @Override
    public String toString() {
        return "MapCounter:\n" + map.entrySet().stream().map(
                o -> String.format("%s : %d", o.getKey(), o.getValue())).
                collect(Collectors.joining("\n"));

    }
}
