package cc.kostic.mst;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class WorkerService {

	private final BinanceProperties properties;
	private final List<String> subscribedStreams = new CopyOnWriteArrayList<>();
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final HttpClient httpClient = HttpClient.newHttpClient();
	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4, r -> {
		Thread thread = new Thread(r, "binance-worker-pool");
		thread.setDaemon(true);
		return thread;
	});

	private final AtomicBoolean running = new AtomicBoolean();
	private final AtomicLong workCycles = new AtomicLong();
	private final AtomicLong generation = new AtomicLong();

	private final List<StreamSession> activeSessions = new CopyOnWriteArrayList<>();
	private volatile String lastError;
	private volatile Instant startedAt;
	private volatile Instant lastWorkAt;

	public WorkerService(BinanceProperties properties) {
		this.properties = properties;
	}

	@PostConstruct
	void initialize() {
		start();
	}

	public synchronized void start() {
		if (running.get()) {
			return;
		}
		long currentGeneration = generation.incrementAndGet();
		running.set(true);
		startedAt = Instant.now();
		lastError = null;
		subscribedStreams.clear();
		activeSessions.clear();

		List<StreamSubscription> subscriptions = subscriptions();
		for (StreamSubscription sub : subscriptions) {
			StreamSession session = new StreamSession(sub, currentGeneration);
			activeSessions.add(session);
			executor.submit(session::run);
		}
	}

	public synchronized void stop() {
		generation.incrementAndGet();
		running.set(false);
		subscribedStreams.clear();
		for (StreamSession session : activeSessions) {
			session.close();
		}
		activeSessions.clear();
	}

	public synchronized void restart() {
		stop();
		start();
	}

	public WorkerStatus status() {
		boolean connected = running.get() && !activeSessions.isEmpty() && activeSessions.stream().anyMatch(StreamSession::isConnected);
		return new WorkerStatus(
				running.get(),
				connected,
				startedAt,
				lastWorkAt,
				workCycles.get(),
				lastError,
				new ArrayList<>(subscribedStreams)
		);
	}

	public List<String> getSubscribedStreams() {
		return new ArrayList<>(subscribedStreams);
	}

	private boolean isCurrent(long currentGeneration) {
		return running.get() && generation.get() == currentGeneration;
	}

	private final class StreamSession {
		private final StreamSubscription subscription;
		private final long sessionGeneration;
		private volatile WebSocket socket;
		private volatile CompletableFuture<WebSocket> connectionAttempt;
		private volatile boolean connected;

		private StreamSession(StreamSubscription subscription, long sessionGeneration) {
			this.subscription = subscription;
			this.sessionGeneration = sessionGeneration;
		}

		boolean isConnected() {
			return connected;
		}

		void close() {
			connected = false;
			for (String stream : subscription.streams()) {
				subscribedStreams.remove(stream);
			}
			CompletableFuture<WebSocket> attempt = connectionAttempt;
			if (attempt != null) {
				attempt.cancel(true);
			}
			WebSocket ws = socket;
			if (ws != null) {
				try {
					ws.sendClose(WebSocket.NORMAL_CLOSURE, "worker stopped");
				} catch (RuntimeException ignored) {
					ws.abort();
				}
				socket = null;
			}
		}

		void run() {
			while (isCurrent(sessionGeneration)) {
				CountDownLatch connectionEnded = new CountDownLatch(1);
				try {
					CompletableFuture<WebSocket> attempt = httpClient.newWebSocketBuilder()
							.buildAsync(URI.create(subscription.url()), new CommonStreamListener(this, connectionEnded, sessionGeneration, subscription));
					connectionAttempt = attempt;
					if (!isCurrent(sessionGeneration)) {
						attempt.cancel(true);
						return;
					}
					WebSocket connectedSocket = attempt.join();
					if (!isCurrent(sessionGeneration)) {
						connectedSocket.abort();
						return;
					}
					socket = connectedSocket;
					connectionEnded.await();
				} catch (Exception exception) {
					if (isCurrent(sessionGeneration)) {
						lastError = exception.getMessage();
						System.err.println("Binance websocket connection failed for " + subscription.streams() + ": " + exception.getMessage());
					}
				} finally {
					connected = false;
					for (String stream : subscription.streams()) {
						subscribedStreams.remove(stream);
					}
					socket = null;
					connectionAttempt = null;
				}
				if (isCurrent(sessionGeneration)) {
					try {
						Thread.sleep(2_000);
					} catch (InterruptedException interrupted) {
						Thread.currentThread().interrupt();
						return;
					}
				}
			}
		}
	}

	private final class CommonStreamListener implements WebSocket.Listener {
		private final StreamSession session;
		private final CountDownLatch connectionEnded;
		private final long listenerGeneration;
		private final StreamSubscription subscription;
		private final StringBuilder message = new StringBuilder();

		private CommonStreamListener(StreamSession session, CountDownLatch connectionEnded, long listenerGeneration, StreamSubscription subscription) {
			this.session = session;
			this.connectionEnded = connectionEnded;
			this.listenerGeneration = listenerGeneration;
			this.subscription = subscription;
		}

		@Override
		public void onOpen(WebSocket webSocket) {
			if (!isCurrent(listenerGeneration)) {
				webSocket.abort();
				connectionEnded.countDown();
				return;
			}
			session.connected = true;
			lastError = null;
			subscribe(webSocket, subscription);
			WebSocket.Listener.super.onOpen(webSocket);
		}

		@Override
		public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
			message.append(data);
			if (last) {
				printCandle(message.toString());
				message.setLength(0);
			}
			return WebSocket.Listener.super.onText(webSocket, data, last);
		}

		@Override
		public void onError(WebSocket webSocket, Throwable error) {
			if (isCurrent(listenerGeneration)) {
				lastError = error.getMessage();
				session.connected = false;
				for (String stream : subscription.streams()) {
					subscribedStreams.remove(stream);
				}
			}
			System.err.println("Binance websocket error for " + subscription.streams() + ": " + error.getMessage());
			connectionEnded.countDown();
		}

		@Override
		public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
			if (isCurrent(listenerGeneration)) {
				session.connected = false;
				for (String stream : subscription.streams()) {
					subscribedStreams.remove(stream);
				}
			}
			connectionEnded.countDown();
			return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
		}
	}

	private void subscribe(WebSocket webSocket, StreamSubscription subscription) {
		String params = subscription.streams().stream()
				.map(s -> "\"" + s + "\"")
				.reduce((a, b) -> a + "," + b)
				.orElse("");
		String command = "{\"method\":\"SUBSCRIBE\",\"params\":[" + params + "],\"id\":" + subscription.id() + "}";
		webSocket.sendText(command, true);
		for (String stream : subscription.streams()) {
			if (!subscribedStreams.contains(stream)) {
				subscribedStreams.add(stream);
			}
		}
		System.out.println("Subscribed to " + subscription.url() + " (streams: " + subscription.streams() + ")");
	}

	private List<StreamSubscription> subscriptions() {
		List<StreamSubscription> result = new ArrayList<>();
		int id = 1;
		if (properties.coinM() != null && properties.coinM().streams() != null && !properties.coinM().streams().isEmpty()) {
			result.add(new StreamSubscription(id++, properties.coinM().streams(), resolveEndpointUrl(properties.coinM().baseUrl())));
		}
		if (properties.perpetuals() != null && properties.perpetuals().streams() != null && !properties.perpetuals().streams().isEmpty()) {
			result.add(new StreamSubscription(id++, properties.perpetuals().streams(), resolveEndpointUrl(properties.perpetuals().baseUrl())));
		}
		return result;
	}

	private String resolveEndpointUrl(String baseUrl) {
		if (baseUrl == null || baseUrl.isBlank()) {
			return "";
		}
		String url = baseUrl.trim();
		if (url.contains("?")) {
			url = url.substring(0, url.indexOf('?'));
		}
		while (url.endsWith("/")) {
			url = url.substring(0, url.length() - 1);
		}
		if (url.endsWith("/market/stream")) {
			url = url.substring(0, url.length() - "/market/stream".length()) + "/ws";
		}
		return url;
	}

	private void printCandle(String message) {
		try {
			JsonNode root = objectMapper.readTree(message);
			JsonNode candle = root.path("k");
			if (candle.isMissingNode() && root.hasNonNull("data")) {
				candle = root.path("data").path("k");
			}
			if (candle.isMissingNode() || !candle.hasNonNull("t") || !candle.hasNonNull("c")) {
				return;
			}
			String symbol = candle.hasNonNull("s") ? candle.path("s").asText() : "BTCUSD";
			System.out.printf("%s candle %s open=%s high=%s low=%s close=%s volume=%s closed=%s%n",
					symbol,
					Instant.ofEpochMilli(candle.path("t").asLong()), candle.path("o").asText(),
					candle.path("h").asText(), candle.path("l").asText(), candle.path("c").asText(),
					candle.path("v").asText(), candle.path("x").asBoolean());
			lastWorkAt = Instant.now();
			workCycles.incrementAndGet();
		} catch (Exception exception) {
			System.err.println("Could not parse Binance candle: " + exception.getMessage());
		}
	}

	@PreDestroy
	void shutdown() {
		stop();
		executor.shutdownNow();
		try {
			executor.awaitTermination(2, TimeUnit.SECONDS);
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	public record WorkerStatus(
			boolean running,
			boolean connected,
			Instant startedAt,
			Instant lastWorkAt,
			long workCycles,
			String lastError,
			List<String> subscribedStreams
	) {
	}

	private record StreamSubscription(int id, List<String> streams, String url) {
	}
}
