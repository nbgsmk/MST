package cc.kostic.mst;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(BinanceProperties.class)
public class AppBinance {

	public static void main(String[] args) {
		SpringApplication.run(AppBinance.class, args);
	}
}
