package reactor.tools.agent;

import org.junit.Test;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorDebugAgentTest {

	static {
		ReactorDebugAgent.init();
	}

	@Test
	public void stepNameAndToString() {
		int baseline = getBaseline();
		Flux<Integer> flux = Flux.just(1);

		assertThat(Scannable.from(flux).stepName())
				.startsWith("Flux.just ⇢ reactor.tools.agent.ReactorDebugAgentTest.stepNameAndToString(ReactorDebugAgentTest.java:" + (baseline + 1));
	}

	private int getBaseline() {
		return new Exception().getStackTrace()[1].getLineNumber();
	}
}