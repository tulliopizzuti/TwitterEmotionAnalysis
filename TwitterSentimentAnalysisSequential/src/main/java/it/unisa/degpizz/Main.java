package it.unisa.degpizz;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import java.io.*;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

public class Main {
    private static final List<String> langToParse= Arrays.asList("en");

    public static void main(String[] args) throws Exception {
        int counterParsed=0;
        if (args == null || args.length < 1) {
            System.out.println("Inserire il path contenente i file .json");
            return;
        }
        String mainPath = args[0];
        File mainDir = new File(mainPath);
        if (!mainDir.isDirectory()) {
            System.out.println("Il path non è una directory");
            return;
        }
        File[] jsonFiles = mainDir.listFiles((file, s) -> s.endsWith(".json"));
        if (jsonFiles == null || jsonFiles.length <= 0) {
            System.out.println("Non sono presenti file .json nel path selezionato");
            return;
        }
        MapCounter mapCounter = new MapCounter();
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();

        for (File f : jsonFiles) {
            JsonStreamParser jsonStreamParser = new JsonStreamParser(new BufferedReader(new FileReader(f)));
            while (jsonStreamParser.hasNext()) {
                JsonElement element = jsonStreamParser.next();
                Tweet tweets = gson.fromJson(element, Tweet.class);
                if(langToParse.contains(tweets.getLang())){
                    mapCounter.parseText(tweets.getText());
                    System.out.println(++counterParsed);
                }
            }

        }
        System.out.println( mapCounter.toString());


    }
}