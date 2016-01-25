package reactor.core.test;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.fn.Consumer;

/**
 * @author Anatoly Kadyshev
 * @author Brian Clozel
 * @author Sebastien Deleuze
 */
public class TestSubscriberTests {

	@Test
	public void testAssertNextValues() {
		TestSubscriber<String> subscriber = new TestSubscriber<>(1);
		Flux.fromIterable(Arrays.asList("1", "2"))
		    .log()
		    .subscribe(subscriber);
		subscriber.awaitAndAssertValues("1");
		subscriber.request(1);
		subscriber.awaitAndAssertValues("2");
	}

	@Test
	public void testAssertNextValuesWith() {
		TestSubscriber<Long> subscriber = new TestSubscriber<>(1);
		Consumer<Long> greaterThanZero = new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				Assert.assertTrue(aLong > 0L);
			}
		};
		Flux.fromIterable(Arrays.asList(1L, 2L)).log().subscribe(subscriber);
		subscriber.awaitAndAssertValuesWith(greaterThanZero);
		subscriber.request(1);
		subscriber.awaitAndAssertValuesWith(greaterThanZero);
	}

	@Test(expected = AssertionError.class)
	public void testAssertNextValuesWithFailure() {
		TestSubscriber<Long> subscriber = new TestSubscriber<>(1);
		Flux.fromIterable(Arrays.asList(1L, 20L))
				.log()
				.subscribe(subscriber);
		Consumer<Long> lowerThanTen = new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				Assert.assertTrue(aLong < 10L);
			}
		};
		subscriber.awaitAndAssertValuesWith(lowerThanTen);
		subscriber.request(1);
		subscriber.awaitAndAssertValuesWith(lowerThanTen);
	}

}