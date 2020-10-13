package reactor.core.scheduler;

import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;
import reactor.blockhound.integration.BlockHoundIntegration;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorBlockHoundIntegrationSPITest {

	@Test
	public void shouldSupportServiceLoader() {
		assertThat(ServiceLoader.load(BlockHoundIntegration.class))
				.hasAtLeastOneElementOfType(ReactorBlockHoundIntegration.class);
	}
}
