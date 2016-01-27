package reactor.core.test;

import java.io.IOException;
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
	public void assertSubscribed() {
		TestSubscriber<String> ts = new TestSubscriber<>();
		ts.assertNotSubscribed();
		Flux.just("foo").subscribe(ts);
		ts.assertSubscribed();
	}

	@Test
	public void assertValues() {
		TestSubscriber<String> ts = new TestSubscriber<>();
		Flux.<String>empty().subscribe(ts);
		ts.assertNoValues();

		ts = new TestSubscriber<>(0);
		Flux.just("foo", "bar").subscribe(ts);
		ts.assertNoValues();
		ts.request(1);
		ts.assertValueCount(1);
		ts.assertValues("foo");
		ts.request(1);
		ts.assertValueCount(2);
		ts.assertValues("foo", "bar");
	}

	@Test(expected = AssertionError.class)
	public void assertValuesNotSameValue() {
		TestSubscriber<String> ts = new TestSubscriber<>();
		Flux.just("foo").subscribe(ts);
		ts.assertValues("bar");
	}

	@Test(expected = AssertionError.class)
	public void assertValuesNotSameCount() {
		TestSubscriber<String> ts = new TestSubscriber<>();
		Flux.just("foo", "foo").subscribe(ts);
		ts.assertValues("foo");
	}

	@Test
	public void assertValuesWith() {
		TestSubscriber<String> ts = new TestSubscriber<>();
		Flux.just("foo", "bar").subscribe(ts);
		ts.assertValuesWith(value -> value.equals("foo"), value -> value.equals("bar"));
	}

	@Test(expected = AssertionError.class)
	public void assertValuesWithFailure() {
		TestSubscriber<String> ts = new TestSubscriber<>();
		Flux.just("foo", "bar").subscribe(ts);
		ts.assertValuesWith(value -> Assert.assertEquals("foo", value), value -> Assert.assertEquals("foo", value));
	}

	@Test(expected = AssertionError.class)
	public void assertValueSequence() {
		TestSubscriber<String> ts = new TestSubscriber<>();
		Flux.just("foo", "bar").subscribe(ts);
		ts.assertValueSequence(Arrays.asList("foo", "bar"));
	}

	@Test
	public void assertValueSequenceFailure() {
		TestSubscriber<String> ts = new TestSubscriber<>();
		Flux.just("foo", "bar").subscribe(ts);
		ts.assertValueSequence(Arrays.asList("foo", "foo"));
	}

	@Test
	public void assertComplete() {
		TestSubscriber<String> ts = new TestSubscriber<>(0);
		Flux.just("foo").subscribe(ts);
		ts.assertNotComplete();
		ts.request(1);
		ts.assertComplete();
	}

	@Test
	public void assertError() {
		TestSubscriber<String> ts = new TestSubscriber<>();
		Flux.just("foo").subscribe(ts);
		ts.assertNoError();

		ts = new TestSubscriber<>(0);
		Flux.<String>error(new IllegalStateException()).subscribe(ts);
		ts.assertError();
		ts.assertError(IllegalStateException.class);
		try {
			ts.assertError(IOException.class);
		}
		catch (AssertionError e) {
			Assert.assertNotNull(e);
		}
		catch(Throwable e) {
			Assert.fail();
		}
	}

	@Test
	public void assertTerminated() {
		TestSubscriber<String> ts = new TestSubscriber<>();
		Flux.<String>error(new IllegalStateException()).subscribe(ts);
		ts.assertTerminated();

		ts = new TestSubscriber<>(0);
		Flux.just("foo").subscribe(ts);
		ts.assertNotTerminated();
		ts.request(1);
		ts.assertTerminated();
	}

	@Test
	public void awaitAndAssertValues() {
		TestSubscriber<String> ts = new TestSubscriber<>(1);
		Flux.just("1", "2").log().subscribe(ts);
		ts.awaitAndAssertValues("1");
		ts.request(1);
		ts.awaitAndAssertValues("2");
	}

	@Test
	public void awaitAndAssertValuesWith() {
		TestSubscriber<Long> ts = new TestSubscriber<>(1);
		Consumer<Long> greaterThanZero = new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				Assert.assertTrue(aLong > 0L);
			}
		};
		Flux.just(1L, 2L).log().subscribe(ts);
		ts.awaitAndAssertValuesWith(greaterThanZero);
		ts.request(1);
		ts.awaitAndAssertValuesWith(greaterThanZero);
	}

	@Test(expected = AssertionError.class)
	public void awaitAndAssertValuesWithFailure() {
		TestSubscriber<Long> ts = new TestSubscriber<>(1);
		Flux.just(1L, 20L).log().subscribe(ts);
		Consumer<Long> lowerThanTen = new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				Assert.assertTrue(aLong < 10L);
			}
		};
		ts.awaitAndAssertValuesWith(lowerThanTen);
		ts.request(1);
		ts.awaitAndAssertValuesWith(lowerThanTen);
	}

	@Test
	public void awaitAndAssertValueCount() {
		TestSubscriber<Long> ts = new TestSubscriber<>(1);
		Flux.just(1L, 2L).log().subscribe(ts);
		ts.awaitAndAssertValueCount(1);
		ts.request(1);
		ts.awaitAndAssertValueCount(1);
	}

	@Test(expected = AssertionError.class)
	public void awaitAndAssertValueCountFailure() {
		TestSubscriber<Long> subscriber = new TestSubscriber<>();
		Flux.just(1L).log().subscribe(subscriber);
		subscriber.configureValuesTimeout(1).awaitAndAssertValueCount(2);
	}

}