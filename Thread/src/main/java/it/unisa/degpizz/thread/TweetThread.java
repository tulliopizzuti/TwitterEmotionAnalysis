package it.unisa.degpizz.thread;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;


import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TweetThread extends Thread {
    private final Logger LOGGER = Logger.getLogger(TweetThread.class.getName());

    private final List<String> LANGTOPARSE = Arrays.asList("en");

    private AtomicInteger counterTextParsed;
    private AtomicInteger counterFileParsed;
    private AtomicInteger parseTextError;
    private AtomicInteger fileOpenError;
    private File[] filesToParse;
    private MapCounter mapCounter;
    private Gson gson;
    private String name;

    public TweetThread(String name, MapCounter mapCounter, AtomicInteger counterTextParsed, AtomicInteger counterFileParsed, AtomicInteger parseTextError, AtomicInteger fileOpenError, File[] filesToParse) {
        this.name = name;
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
        LOGGER.log(Level.INFO, String.format("Avviato il thread: %s", name));
        for (File f : filesToParse) {
            LOGGER.log(Level.INFO, String.format("Thread: %s - Inizio file %s", name, f.getName()));

            try (BufferedReader br = getBufferedReaderForCompressedFile(f)) {
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
                LOGGER.log(Level.INFO, String.format("Thread: %s - Fine file %s", name, f.getName()));

            } catch (Exception e) {
                fileOpenError.incrementAndGet();
                LOGGER.log(Level.SEVERE, String.format("Thread: %s - Errore file %s", name, f.getName()));

            }
            counterFileParsed.incrementAndGet();

        }
        LOGGER.log(Level.INFO, String.format("Terminato il thread: %s", name));

    }

    public BufferedReader getBufferedReaderForCompressedFile(File fileIn) throws FileNotFoundException, CompressorException {

        return new BufferedReader(new InputStreamReader(new CompressorStreamFactory().createCompressorInputStream(new BufferedInputStream(new FileInputStream(fileIn)))));
    }
}
