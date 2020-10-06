package it.unisa.degpizz.preprocessingdataset;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Main {
    private static final List<String> LANGTOPARSE = Arrays.asList("en");
    private static final DateFormat DF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss:SSS");
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        if (args == null || args.length < 3) {
            LOGGER.log(Level.SEVERE, "<inputfolder> <outputfolder> <depth>");
            return;
        }
        String inputPath = args[0];
        File inputDir = new File(inputPath);
        if (!inputDir.isDirectory()) {
            LOGGER.log(Level.SEVERE, "Il path <inputfolder> non è una directory");
            return;
        }
        String outputPath = args[1];
        File outputDir = new File(outputPath);
        if (!outputDir.exists()) {
            outputDir.mkdir();
        }
        if (!outputDir.isDirectory()) {
            LOGGER.log(Level.SEVERE, "Il path <outputfolder> non è una directory");
            return;
        }
        String depthString = args[2];
        int maxDepth = 0;
        try {
            maxDepth = Integer.parseInt(depthString);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Depth deve essere un intero");
            return;
        }
        long millisStart=System.currentTimeMillis();
        LOGGER.log(Level.INFO,String.format("Inizio: %s", DF.format(new Date(millisStart))));

        int fileParsed = parseFolder(inputDir.getName(), inputDir, 0, maxDepth, outputPath);
        long millisFinish=System.currentTimeMillis();
        LOGGER.log(Level.INFO,String.format("Fine: %s", DF.format(new Date(millisFinish))));
        LOGGER.log(Level.INFO, "File elaborati: %d", fileParsed);


    }

    private static int parseFolder(String prefix, File dir, int depth, int maxDepth, String outputDir) {
        if (maxDepth < depth) {
            return 0;
        }
        if (dir.isDirectory()) {
            File[] listFiles = dir.listFiles();
            if (listFiles != null && listFiles.length > 0) {
                List<File> files = Arrays.asList(listFiles);
                return files.stream().map(f -> parseFolder(String.format("%s_%s", prefix, f.getName()), f, depth + 1, maxDepth, outputDir)).mapToInt(Integer::intValue).sum();

            }
        }

        return parseFile(dir, prefix, outputDir) ? 1 : 0;
    }

    private static boolean parseFile(File f, String prefix, String outputDir) {
        LOGGER.log(Level.INFO, String.format("Inizio file %s", prefix));
        Gson gson = new GsonBuilder().create();
        String fileName = prefix.replace(".bz2", "");
        File fileOutput = new File(outputDir + File.separator + fileName);
        if (fileOutput.exists()) {
            fileOutput.delete();
        }
        try {
            fileOutput.createNewFile();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, String.format("Errore durante la creazione del file %s", fileName));
            return false;
        }
        try (PrintWriter printWriter = new PrintWriter(fileOutput)) {
            try (BufferedReader br = getBufferedReaderForCompressedFile(f)) {
                JsonStreamParser jsonStreamParser = new JsonStreamParser(br);
                while (jsonStreamParser.hasNext()) {
                    JsonElement jsonElement = jsonStreamParser.next();
                    Tweet tweets = gson.fromJson(jsonElement, Tweet.class);
                    if (LANGTOPARSE.contains(tweets.getLang())) {
                        try {
                            String jsonString = gson.toJson(jsonElement);
                            printWriter.println(jsonString);
                        } catch (Exception e) {
                            LOGGER.log(Level.SEVERE, String.format("Errore nel parsing di un Json in %s", f.getName()));
                            return false;
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, String.format("Errore file %s", f.getName()));
                return false;

            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, String.format("Errore durante la creazione del PrintWriter per %s", fileName));
            return false;
        }




        String fileCompressedOutputName = prefix;
        File compressed = new File(outputDir + File.separator + fileCompressedOutputName);
        if (compressed.exists()) {
            compressed.delete();
        }
        try (
                OutputStream out = new FileOutputStream(compressed);
                CompressorOutputStream cos = new CompressorStreamFactory().createCompressorOutputStream("bzip2", out);
                FileInputStream in = new FileInputStream(fileOutput);
        ) {
            IOUtils.copy(in, cos);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, String.format("Errore durante la compressione di %s", fileName));
            return false;
        }
        fileOutput.delete();
        LOGGER.log(Level.INFO, String.format("Fine file %s", prefix));
        return true;
    }

    public static BufferedReader getBufferedReaderForCompressedFile(File fileIn) throws FileNotFoundException, CompressorException {

        return new BufferedReader(new InputStreamReader(new CompressorStreamFactory().createCompressorInputStream(new BufferedInputStream(new FileInputStream(fileIn)))));
    }
}