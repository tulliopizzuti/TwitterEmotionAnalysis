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

public class Main {
    private static final List<String> LANGTOPARSE = Arrays.asList("en");
    private static final DateFormat DF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss:SSS");

    public static void main(String[] args) {

        int counterTextParsed = 0;
        int counterFileParsed = 0;
        int parseTextError = 0;
        int fileOpenError = 0;
        if (args == null || args.length < 1) {
            System.out.println("Inserire il path contenente i file .bz2");
            return;
        }
        String mainPath = args[0];
        File mainDir = new File(mainPath);
        if (!mainDir.isDirectory()) {
            System.out.println("Il path non è una directory");
            return;
        }
        File[] bz2Files = mainDir.listFiles((file, s) -> s.endsWith(".bz2"));
        if (bz2Files == null || bz2Files.length <= 0) {
            System.out.println("Non sono presenti file .bz2 nel path selezionato");
            return;
        }
        MapCounter mapCounter = new MapCounter();
        Gson gson = new GsonBuilder().create();
        long millisStart = System.currentTimeMillis();
        System.out.println(String.format("Inizio: %s", DF.format(new Date(millisStart))));
        for (File f : bz2Files) {
            try (BufferedReader br = getBufferedReaderForCompressedFile(f)) {
                JsonStreamParser jsonStreamParser = new JsonStreamParser(br);
                while (jsonStreamParser.hasNext()) {
                    Tweet tweets = gson.fromJson(jsonStreamParser.next(), Tweet.class);
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
        System.out.println(String.format("Fine: %s", DF.format(new Date(millisFinish))));
        long millis = (millisFinish - millisStart) % 1000;
        long seconds = (millisFinish - millisStart) / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        long days = hours / 24;
        System.out.println(String.format("Tempo impiegato: %02d:%02d:%02d:%02d:%03d", days, hours, minutes, seconds, millis));
        System.out.println(mapCounter.toString());
        System.out.println(String.format("Successi parseText: %d", counterTextParsed));
        System.out.println(String.format("Successi openFile: %d", counterFileParsed));
        System.out.println(String.format("Errori parseText: %d", parseTextError));
        System.out.println(String.format("Errori openFile: %d", fileOpenError));
    }

    public static BufferedReader getBufferedReaderForCompressedFile(File fileIn) throws FileNotFoundException, CompressorException {

        return new BufferedReader(new InputStreamReader(new CompressorStreamFactory().createCompressorInputStream(new BufferedInputStream(new FileInputStream(fileIn)))));
    }
}