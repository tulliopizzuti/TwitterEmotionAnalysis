package it.unisa.degpizz.threadinformation;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class InformationCounter {
    private AtomicInteger totalCounter;
    private AtomicInteger enCounter;
    private final List<String> LANGTOPARSE = Arrays.asList("en");

    public InformationCounter() {
        totalCounter = new AtomicInteger(0);
        enCounter = new AtomicInteger(0);

    }

    public synchronized void parseTweet(Tweet tweet) {
        totalCounter.incrementAndGet();
        if (LANGTOPARSE.contains(tweet.getLang())) {
            enCounter.incrementAndGet();
        }
    }

    @Override
    public String toString() {
        return String.format("TOTAL: %d\nENGLISH: %d",totalCounter.get(),enCounter.get());
    }
}
