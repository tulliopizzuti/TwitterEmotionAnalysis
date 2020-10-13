package it.unisa.degpizz.sequential;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final DateFormat DF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss:SSS");
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {

        int counterTextParsed = 0;
        int counterFileParsed = 0;
        int parseTextError = 0;
        int fileOpenError = 0;
        if (args == null || args.length < 1) {
            LOGGER.log(Level.SEVERE, "Inserire il path contenente i file .bz2");
            return;
        }
        String mainPath = args[0];
        File mainDir = new File(mainPath);
        if (!mainDir.isDirectory()) {
            LOGGER.log(Level.SEVERE, "Il path non è una directory");
            return;
        }
        File[] bz2Files = mainDir.listFiles((file, s) -> s.endsWith(".bz2"));
        if (bz2Files == null || bz2Files.length <= 0) {
            LOGGER.log(Level.SEVERE,"Non sono presenti file .bz2 nel path selezionato");
            return;
        }
        MapCounter mapCounter = new MapCounter();
        Gson gson = new GsonBuilder().create();
        long millisStart = System.currentTimeMillis();
        LOGGER.log(Level.INFO,String.format("Inizio: %s", DF.format(new Date(millisStart))));

        for (File f : bz2Files) {
            LOGGER.log(Level.INFO,String.format("Inizio file %s",f.getName()));

            try (BufferedReader br = getBufferedReaderForCompressedFile(f)) {
                JsonStreamParser jsonStreamParser = new JsonStreamParser(br);
                while (jsonStreamParser.hasNext()) {
                    Tweet tweets = gson.fromJson(jsonStreamParser.next(), Tweet.class);
                        try {
                            mapCounter.parseText(tweets.getText());
                            counterTextParsed++;
                        } catch (Exception e) {
                            parseTextError++;
                        }


                }
            } catch (Exception e) {
                fileOpenError++;
                LOGGER.log(Level.SEVERE,String.format("Errore file %s",f.getName()));
            }
            LOGGER.log(Level.INFO,String.format("Fine file %s ",f.getName()));
            counterFileParsed++;
            break;
        }



        long millisFinish = System.currentTimeMillis();
        LOGGER.log(Level.INFO,String.format("Fine: %s", DF.format(new Date(millisFinish))));

        long millis = (millisFinish - millisStart) % 1000;
        long seconds = (millisFinish - millisStart) / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        long days = hours / 24;
        LOGGER.log(Level.INFO,String.format("Tempo impiegato: %02d:%02d:%02d:%02d:%03d", days, hours, minutes, seconds, millis));


        LOGGER.log(Level.INFO,mapCounter.toString());
        LOGGER.log(Level.INFO,String.format("Successi parseText: %d", counterTextParsed));
        LOGGER.log(Level.INFO,String.format("Successi openFile: %d", counterFileParsed));
        LOGGER.log(Level.INFO,String.format("Errori parseText: %d", parseTextError));
        LOGGER.log(Level.INFO,String.format("Errori openFile: %d", fileOpenError));
    }

    public static BufferedReader getBufferedReaderForCompressedFile(File fileIn) throws FileNotFoundException, CompressorException {

        return new BufferedReader(new InputStreamReader(new CompressorStreamFactory().createCompressorInputStream(new BufferedInputStream(new FileInputStream(fileIn)))));
    }
}