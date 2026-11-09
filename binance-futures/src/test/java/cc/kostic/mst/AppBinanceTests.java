package cc.kostic.mst;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = AppBinance.class)
class AppBinanceTests {

	@Autowired
	private BinanceProperties properties;

	@Autowired
	private WorkerService workerService;

	@Test
	void contextAndPropertiesLoad() {
		assertNotNull(properties);
		assertNotNull(properties.coinM());
		assertNotNull(properties.perpetuals());
		assertEquals(2, properties.coinM().streams().size());
		assertEquals(2, properties.perpetuals().streams().size());

		WorkerService.WorkerStatus status = workerService.status();
		assertNotNull(status);
		assertTrue(status.running());
	}
}
