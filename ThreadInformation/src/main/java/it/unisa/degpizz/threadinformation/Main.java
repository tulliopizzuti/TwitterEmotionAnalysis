package it.unisa.degpizz.threadinformation;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final DateFormat DF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss:SSS");
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        int N_THREADS = 2;
        AtomicInteger counterTextParsed = new AtomicInteger(0);
        AtomicInteger counterFileParsed = new AtomicInteger(0);
        AtomicInteger parseTextError = new AtomicInteger(0);
        AtomicInteger fileOpenError = new AtomicInteger(0);
        if (args == null || args.length < 1) {
            LOGGER.log(Level.SEVERE, "Inserire il path contenente i file .bz2");
            return;
        }
        if (args.length > 2) {
            LOGGER.log(Level.SEVERE, "Troppi parametri");
            return;
        }
        if (args.length == 2) {
            try {
                N_THREADS = Integer.parseInt(args[1]);

            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Non è stato possibile convertire il parametro in intero");
                return;
            }
        }
        String mainPath = args[0];
        File mainDir = new File(mainPath);
        if (!mainDir.isDirectory()) {
            LOGGER.log(Level.SEVERE, "Il path non è una directory");
            return;
        }
        File[] jsonFiles = mainDir.listFiles((file, s) -> s.endsWith(".bz2"));
        if (jsonFiles == null || jsonFiles.length <= 0) {
            LOGGER.log(Level.SEVERE, "Non sono presenti file .bz2 nel path selezionato");
            return;
        }
        if (jsonFiles.length < N_THREADS) {
            LOGGER.log(Level.SEVERE, "Il numero di file è minore del numero di Thread");
            return;
        }


        InformationCounter informationCounter = new InformationCounter();
        List<TweetThread> threadArrays = new ArrayList<>();
        int division = jsonFiles.length / N_THREADS;
        for (int i = 0; i < N_THREADS; i++) {
            int indexStart = i * division;
            int indexFinish = indexStart + division;
            if (i == (N_THREADS - 1)) {
                indexFinish = jsonFiles.length;
            }
            threadArrays.add(new TweetThread("Thread" + i, informationCounter, counterTextParsed,
                    counterFileParsed, parseTextError, fileOpenError, Arrays.copyOfRange(jsonFiles, indexStart, indexFinish)));
        }


        long millisStart = System.currentTimeMillis();
        LOGGER.log(Level.INFO, String.format("Inizio: %s", DF.format(new Date(millisStart))));
        threadArrays.forEach(Thread::start);
        for (TweetThread t : threadArrays) {
            try {
                t.join();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, String.format("Il Thread %s è crashato", t.getName()));
            }
        }

        long millisFinish = System.currentTimeMillis();
        LOGGER.log(Level.INFO, String.format("Fine: %s", DF.format(new Date(millisFinish))));



        long millis = (millisFinish - millisStart) % 1000;
        long seconds = (millisFinish - millisStart) / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        long days = hours / 24;
        LOGGER.log(Level.INFO,String.format("Tempo impiegato: %02d:%02d:%02d:%02d:%03d", days, hours, minutes, seconds, millis));
        LOGGER.log(Level.INFO, informationCounter.toString());
        LOGGER.log(Level.INFO,String.format("Successi parseText: %d", counterTextParsed.get()));
        LOGGER.log(Level.INFO,String.format("Successi openFile: %d", counterFileParsed.get()));
        LOGGER.log(Level.INFO,String.format("Errori parseText: %d", parseTextError.get()));
        LOGGER.log(Level.INFO,String.format("Errori openFile: %d", fileOpenError.get()));
    }

}
