package reactor.core.subscriber.test;

import java.util.Arrays;

import org.junit.Test;
import reactor.Flux;

/**
 * @author Anatoly Kadyshev
 */
public class DataTestSubscriberTest {

	@Test
	public void testAssertNextSignals() throws Exception {
		DataTestSubscriber<String> subscriber = DataTestSubscriber.createWithTimeoutSecs(1);
		Flux.fromIterable(Arrays.asList("1", "2"))
		    .log()
		    .subscribe(subscriber);

		subscriber.request(1);

		subscriber.assertNextSignals("1");

		subscriber.request(1);

		subscriber.assertNextSignals("2");
	}

}