package it.unisa.degpizz.thread;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final DateFormat DF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss:SSS");


    public static void main(String[] args) {
        int N_THREADS = 2;
        AtomicInteger counterTextParsed = new AtomicInteger(0);
        AtomicInteger counterFileParsed = new AtomicInteger(0);
        AtomicInteger parseTextError = new AtomicInteger(0);
        AtomicInteger fileOpenError = new AtomicInteger(0);
        if (args == null || args.length < 1) {
            System.out.println("Inserire il path contenente i file .json");
            return;
        }
        if (args.length > 2) {
            System.out.println("Troppi parametri");
            return;
        }
        if (args.length == 2) {
            try {
                N_THREADS = Integer.parseInt(args[1]);

            } catch (Exception ex) {
                System.out.println("Non è stato possibile convertire il parametro in intero");
                return;
            }
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
        if (jsonFiles.length < N_THREADS) {
            System.out.println("Il numero di file è minore del numero di Thread");
            return;
        }
        MapCounter mapCounter = new MapCounter();
        List<TweetThread> threadArrays = new ArrayList<>();
        int division = jsonFiles.length / N_THREADS;
        for (int i = 0; i < N_THREADS; i++) {
            int indexStart = i * division;
            int indexFinish = indexStart + division;
            if (i == (N_THREADS - 1)) {
                indexFinish = jsonFiles.length;
            }
            threadArrays.add(new TweetThread("Thread" + i, mapCounter, counterTextParsed,
                    counterFileParsed, parseTextError, fileOpenError, Arrays.copyOfRange(jsonFiles, indexStart, indexFinish)));
        }


        long millisStart = System.currentTimeMillis();
        System.out.println(String.format("Inizio: %s", DF.format(new Date(millisStart))));
        threadArrays.forEach(Thread::start);
        for (TweetThread t : threadArrays) {
            try {
                t.join();
            } catch (Exception e) {
                System.out.println(String.format("Il Thread %s è crashato", t.getName()));
            }
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
        System.out.println(String.format("Successi parseText: %d", counterTextParsed.get()));
        System.out.println(String.format("Successi openFile: %d", counterFileParsed.get()));
        System.out.println(String.format("Errori parseText: %d", parseTextError.get()));
        System.out.println(String.format("Errori openFile: %d", fileOpenError.get()));
    }

}
