package cc.kostic.mst;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class WorkerService {
    private static final URI BINANCE_STREAM = URI.create("wss://dstream.binance.com/ws/btcusd_perp@kline_1m");
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "binance-btcusd-worker");
        thread.setDaemon(false);
        return thread;
    });
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong workCycles = new AtomicLong();
    private final AtomicLong generation = new AtomicLong();
    private volatile Future<?> worker;
    private volatile WebSocket socket;
    private volatile CompletableFuture<WebSocket> connectionAttempt;
    private volatile boolean connected;
    private volatile String lastError;
    private volatile Instant startedAt;
    private volatile Instant lastWorkAt;

    @PostConstruct
    void initialize() { start(); }

    public synchronized void start() {
        if (running.get()) return;
        long currentGeneration = generation.incrementAndGet();
        running.set(true);
        startedAt = Instant.now();
        lastError = null;
        worker = executor.submit(() -> streamForever(currentGeneration));
    }

    public synchronized void stop() {
        generation.incrementAndGet();
        running.set(false);
        connected = false;
        WebSocket currentSocket = socket;
        if (currentSocket != null) {
            try {
                currentSocket.sendClose(WebSocket.NORMAL_CLOSURE, "worker stopped");
            } catch (RuntimeException ignored) {
                currentSocket.abort();
            }
            socket = null;
        }
        CompletableFuture<WebSocket> currentAttempt = connectionAttempt;
        if (currentAttempt != null) currentAttempt.cancel(true);
        Future<?> currentWorker = worker;
        if (currentWorker != null) {
            currentWorker.cancel(true);
            worker = null;
        }
    }

    public synchronized void restart() { stop(); start(); }

    public WorkerStatus status() {
        return new WorkerStatus(running.get(), connected, startedAt, lastWorkAt, workCycles.get(), lastError);
    }

    private void streamForever(long currentGeneration) {
        while (isCurrent(currentGeneration)) {
            CountDownLatch connectionEnded = new CountDownLatch(1);
            try {
                CompletableFuture<WebSocket> attempt = httpClient.newWebSocketBuilder()
                        .buildAsync(BINANCE_STREAM, new BinanceListener(connectionEnded, currentGeneration));
                connectionAttempt = attempt;
                if (!isCurrent(currentGeneration)) {
                    attempt.cancel(true);
                    return;
                }
                WebSocket connectedSocket = attempt.join();
                if (!isCurrent(currentGeneration)) {
                    connectedSocket.abort();
                    return;
                }
                socket = connectedSocket;
                connectionEnded.await();
            } catch (Exception exception) {
                if (isCurrent(currentGeneration)) {
                    lastError = exception.getMessage();
                    System.err.println("Binance websocket connection failed: " + exception.getMessage());
                }
            } finally {
                if (generation.get() == currentGeneration) {
                    connected = false;
                    socket = null;
                    connectionAttempt = null;
                }
            }
            if (isCurrent(currentGeneration)) {
                try { Thread.sleep(2_000); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
            }
        }
    }

    private boolean isCurrent(long currentGeneration) {
        return running.get() && generation.get() == currentGeneration;
    }

    private final class BinanceListener implements WebSocket.Listener {
        private final CountDownLatch connectionEnded;
        private final long listenerGeneration;
        private final StringBuilder message = new StringBuilder();
        private BinanceListener(CountDownLatch connectionEnded, long listenerGeneration) {
            this.connectionEnded = connectionEnded;
            this.listenerGeneration = listenerGeneration;
        }

        @Override public void onOpen(WebSocket webSocket) {
            if (!isCurrent(listenerGeneration)) {
                webSocket.abort();
                connectionEnded.countDown();
                return;
            }
            connected = true;
            lastError = null;
            System.out.println("Connected to Binance Futures BTCUSD candle stream");
            WebSocket.Listener.super.onOpen(webSocket);
        }

        @Override public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            message.append(data);
            if (last) {
                printCandle(message.toString());
                message.setLength(0);
            }
            return WebSocket.Listener.super.onText(webSocket, data, last);
        }

        @Override public void onError(WebSocket webSocket, Throwable error) {
            if (isCurrent(listenerGeneration)) lastError = error.getMessage();
            System.err.println("Binance websocket error: " + error.getMessage());
            if (isCurrent(listenerGeneration)) connected = false;
            connectionEnded.countDown();
        }

        @Override public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            if (isCurrent(listenerGeneration)) connected = false;
            connectionEnded.countDown();
            return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
        }
    }

    private void printCandle(String message) {
        try {
            JsonNode candle = objectMapper.readTree(message).path("k");
            if (candle.isMissingNode() || !candle.hasNonNull("t") || !candle.hasNonNull("c")) return;
            System.out.printf("BTCUSD candle %s open=%s high=%s low=%s close=%s volume=%s closed=%s%n",
                    Instant.ofEpochMilli(candle.path("t").asLong()), candle.path("o").asText(),
                    candle.path("h").asText(), candle.path("l").asText(), candle.path("c").asText(),
                    candle.path("v").asText(), candle.path("x").asBoolean());
            lastWorkAt = Instant.now();
            workCycles.incrementAndGet();
        } catch (Exception exception) { System.err.println("Could not parse Binance candle: " + exception.getMessage()); }
    }

    @PreDestroy
    void shutdown() {
        stop();
        executor.shutdownNow();
        try { executor.awaitTermination(2, TimeUnit.SECONDS); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }

    public record WorkerStatus(boolean running, boolean connected, Instant startedAt,
                               Instant lastWorkAt, long workCycles, String lastError) { }
}
