package reactor.core.subscriber.test;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.fn.Consumer;

/**
 * @author Anatoly Kadyshev
 * @author Brian Clozel
 */
public class DataTestSubscriberTest {

	@Test
	public void testAssertNextSignalsEqual() throws Exception {
		DataTestSubscriber<String> subscriber = DataTestSubscriber.createWithTimeoutSecs(1);
		Flux.fromIterable(Arrays.asList("1", "2"))
		    .log()
		    .subscribe(subscriber);

		subscriber.request(1);

		subscriber.assertNextSignalsEqual("1");

		subscriber.request(1);

		subscriber.assertNextSignalsEqual("2");
	}

	@Test
	public void testAssertNextSignals() throws Exception {
		DataTestSubscriber<Long> subscriber = DataTestSubscriber.createWithTimeoutSecs(1);
		Flux.fromIterable(Arrays.asList(1L, 2L))
				.log()
				.subscribe(subscriber);

		Consumer<Long> greaterThanZero = new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				Assert.assertTrue(aLong > 0L);
			}
		};

		subscriber.request(1);
		subscriber.assertNextSignals(greaterThanZero);

		subscriber.request(1);
		subscriber.assertNextSignals(greaterThanZero);
	}

	@Test(expected = AssertionError.class)
	public void testAssertNextSignalsFailure() throws Exception {
		DataTestSubscriber<Long> subscriber = DataTestSubscriber.createWithTimeoutSecs(1);
		Flux.fromIterable(Arrays.asList(1L, 20L))
				.log()
				.subscribe(subscriber);

		Consumer<Long> lowerThanTen = new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				Assert.assertTrue(aLong < 10L);
			}
		};

		subscriber.request(1);
		subscriber.assertNextSignals(lowerThanTen);

		subscriber.request(1);
		subscriber.assertNextSignals(lowerThanTen);
	}

}