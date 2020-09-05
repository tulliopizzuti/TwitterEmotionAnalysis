package it.unisa.degpizz.thread;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TweetThread extends Thread {
    private final List<String> LANGTOPARSE = Arrays.asList("en");

    private AtomicInteger counterTextParsed;
    private AtomicInteger counterFileParsed;
    private AtomicInteger parseTextError;
    private AtomicInteger fileOpenError;
    private File[] filesToParse;
    private MapCounter mapCounter;
    private Gson gson;
    private String name;

    public TweetThread(String name,MapCounter mapCounter, AtomicInteger counterTextParsed, AtomicInteger counterFileParsed, AtomicInteger parseTextError, AtomicInteger fileOpenError, File[] filesToParse) {
        this.name=name;
        this.counterTextParsed = counterTextParsed;
        this.counterFileParsed = counterFileParsed;
        this.parseTextError = parseTextError;
        this.fileOpenError = fileOpenError;
        this.filesToParse = filesToParse;
        this.mapCounter = mapCounter;
        gson = new GsonBuilder().create();
    }

    @Override
    public void run() {
        System.out.println(String.format("Avviato il thread: %s",name));
        for (File f : filesToParse) {
            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                JsonStreamParser jsonStreamParser = new JsonStreamParser(br);
                while (jsonStreamParser.hasNext()) {
                    JsonElement element = jsonStreamParser.next();
                    Tweet tweets = gson.fromJson(element, Tweet.class);
                    if (LANGTOPARSE.contains(tweets.getLang())) {
                        try {
                            mapCounter.parseText(tweets.getText());
                            counterTextParsed.incrementAndGet();
                        } catch (Exception e) {
                            parseTextError.incrementAndGet();
                        }

                    }
                }
            } catch (Exception e) {
                fileOpenError.incrementAndGet();
            }
            counterFileParsed.incrementAndGet();
        }
        System.out.println(String.format("Terminato il thread: %s",name));

    }
}
