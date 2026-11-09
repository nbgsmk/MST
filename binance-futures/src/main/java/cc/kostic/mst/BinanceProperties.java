package cc.kostic.mst;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "binance")
public record BinanceProperties(
		StreamEndpoint coinM,
		StreamEndpoint perpetuals
) {
	public record StreamEndpoint(
			String baseUrl,
			List<String> streams
	) {
		public StreamEndpoint {
			if (streams == null) {
				streams = List.of();
			}
		}
	}
}
