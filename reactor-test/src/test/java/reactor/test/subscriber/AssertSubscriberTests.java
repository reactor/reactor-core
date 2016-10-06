/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.test.subscriber;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;

/**
 * @author Anatoly Kadyshev
 * @author Brian Clozel
 * @author Sebastien Deleuze
 */
public class AssertSubscriberTests {

	@Test
	public void assertSubscribed() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		ts.assertNotSubscribed();
		Flux.just("foo").subscribe(ts);
		ts.assertSubscribed();
	}

	@Test
	public void assertValues() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux.<String>empty().subscribe(ts);
		ts.assertNoValues();

		ts = AssertSubscriber.create(0);
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
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux.just("foo").subscribe(ts);
		ts.assertValues("bar");
	}

	@Test(expected = AssertionError.class)
	public void assertValuesNotSameCount() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux.just("foo", "foo").subscribe(ts);
		ts.assertValues("foo");
	}

	@Test
	public void assertValuesWith() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux.just("foo", "bar").subscribe(ts);
		ts.assertValuesWith(value -> value.equals("foo"), value -> value.equals("bar"));
	}

	@Test(expected = AssertionError.class)
	public void assertValuesWithFailure() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux.just("foo", "bar").subscribe(ts);
		ts.assertValuesWith(value -> Assert.assertEquals("foo", value), value -> Assert.assertEquals("foo", value));
	}

	@Test
	public void assertValueSequence() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux.just("foo", "bar").subscribe(ts);
		ts.assertValueSequence(Arrays.asList("foo", "bar"));
	}

	@Test(expected = AssertionError.class)
	public void assertValueSequenceFailure() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux.just("foo", "bar").subscribe(ts);
		ts.assertValueSequence(Arrays.asList("foo", "foo"));
	}

	@Test
	public void assertComplete() {
		AssertSubscriber<String> ts = AssertSubscriber.create(0);
		Flux.just("foo").subscribe(ts);
		ts.assertNotComplete();
		ts.request(1);
		ts.assertComplete();
	}

	@Test
	public void assertError() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux.just("foo").subscribe(ts);
		ts.assertNoError();

		ts = AssertSubscriber.create(0);
		Flux.<String>error(new IllegalStateException()).subscribe(ts);
		ts.assertError();
		ts.assertError(IllegalStateException.class);
		try {
			ts.assertError(IOException.class);
		}
		catch (AssertionError e) {
			Assert.assertNotNull(e);
			Assert.assertEquals("Error class incompatible: expected = class java.io.IOException, " +
								"actual = java.lang.IllegalStateException", e.getMessage());
		}
		catch(Throwable e) {
			Assert.fail();
		}
	}

	@Test
	public void assertTerminated() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux.<String>error(new IllegalStateException()).subscribe(ts);
		ts.assertTerminated();

		ts = AssertSubscriber.create(0);
		Flux.just("foo").subscribe(ts);
		ts.assertNotTerminated();
		ts.request(1);
		ts.assertTerminated();
	}

	@Test
	public void awaitAndAssertValues() {
		AssertSubscriber<String> ts = AssertSubscriber.create(1);
		Flux.just("1", "2").log().subscribe(ts);
		ts.awaitAndAssertNextValues("1");
		ts.request(1);
		ts.awaitAndAssertNextValues("2");
	}

	@Test(expected = AssertionError.class)
	public void awaitAndAssertNotEnoughValues() {
		AssertSubscriber<String> ts = AssertSubscriber.create(1);
		Flux.just("1", "2").log().subscribe(ts);
		ts.awaitAndAssertNextValues("1", "2");
	}

	@Test
	public void awaitAndAssertAsyncValues() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		Flux.interval(Duration.ofMillis(100)).map(l -> "foo " + l).subscribe(ts);
		try {
		    Thread.sleep(500);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		ts.awaitAndAssertNextValues("foo 0", "foo 1", "foo 2");
	}

	@Test
	public void awaitAndAssertValuesWith() {
		AssertSubscriber<Long> ts = AssertSubscriber.create(1);
		Consumer<Long> greaterThanZero = new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				Assert.assertTrue(aLong > 0L);
			}
		};
		Flux.just(1L, 2L).log().subscribe(ts);
		ts.awaitAndAssertNextValuesWith(greaterThanZero);
		ts.request(1);
		ts.awaitAndAssertNextValuesWith(greaterThanZero);
	}

	@Test(expected = AssertionError.class)
	public void awaitAndAssertValuesWithFailure() {
		AssertSubscriber<Long> ts = AssertSubscriber.create(1);
		Flux.just(1L, 20L).log().subscribe(ts);
		Consumer<Long> lowerThanTen = new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				Assert.assertTrue(aLong < 10L);
			}
		};
		ts.awaitAndAssertNextValuesWith(lowerThanTen);
		ts.request(1);
		ts.awaitAndAssertNextValuesWith(lowerThanTen);
	}

	@Test
	public void awaitAndAssertValueCount() {
		AssertSubscriber<Long> ts = AssertSubscriber.create(1);
		Flux.just(1L, 2L).log().subscribe(ts);
		ts.awaitAndAssertNextValueCount(1);
		ts.request(1);
		ts.awaitAndAssertNextValueCount(1);
	}

	@Test(expected = AssertionError.class)
	public void awaitAndAssertValueCountFailure() {
		AssertSubscriber<Long> subscriber = AssertSubscriber.create();
		Flux.just(1L).log().subscribe(subscriber);
		subscriber.configureValuesTimeout(Duration.ofSeconds(1)).awaitAndAssertNextValueCount(2);
	}

}