package it.unisa.degpizz.sequential;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Main {
    private static final List<String> LANGTOPARSE = Arrays.asList("en");
    private static final DateFormat DF=new SimpleDateFormat("dd/MM/yyyy HH:mm:ss:SSS");
    public static void main(String[] args) {

        int counterTextParsed = 0;
        int counterFileParsed = 0;
        int parseTextError = 0;
        int fileOpenError = 0;
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
        long millisStart = System.currentTimeMillis();
        System.out.println(String.format("Inizio: %s", DF.format(new Date(millisStart))));
        for (File f : jsonFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                JsonStreamParser jsonStreamParser = new JsonStreamParser(br);
                while (jsonStreamParser.hasNext()) {
                    JsonElement element = jsonStreamParser.next();
                    Tweet tweets = gson.fromJson(element, Tweet.class);
                    if (LANGTOPARSE.contains(tweets.getLang())) {
                        try {
                            mapCounter.parseText(tweets.getText());
                            counterTextParsed++;
                        } catch (Exception e) {
                            parseTextError++;
                        }

                    }
                }
            } catch (Exception e) {
                fileOpenError++;
            }
            counterFileParsed++;
        }
        long millisFinish = System.currentTimeMillis();
        System.out.println(String.format("Fine: %s",  DF.format(new Date(millisFinish))));
        long millis = (millisFinish-millisStart) % 1000;
        long seconds = (millisFinish-millisStart) / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        long days = hours / 24;
        System.out.println(String.format("Tempo impiegato: %02d:%02d:%02d:%02d:%03d",days,hours,minutes,seconds,millis  ));
        System.out.println(mapCounter.toString());
        System.out.println(String.format("Successi parseText: %d", counterTextParsed));
        System.out.println(String.format("Successi openFile: %d", counterFileParsed));
        System.out.println(String.format("Errori parseText: %d", parseTextError));
        System.out.println(String.format("Errori openFile: %d", fileOpenError));
    }
}