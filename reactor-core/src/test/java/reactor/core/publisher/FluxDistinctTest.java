/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxDistinctTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ANY)
				.fusionModeThreadBarrier(Fuseable.ANY);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.distinct(d -> {
					throw exception();
				})),

				scenario(f -> f.distinct(d -> null))

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(f -> f.distinct()));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.distinct()).producer(3, i -> item(0))
				                           .receiveValues((item(0)))
				                           .receiverDemand(2),

				scenario(f -> f.distinct())
		);
	}

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxDistinct<>(null, k -> k, HashSet::new);
	}

	@Test(expected = NullPointerException.class)
	public void keyExtractorNull() {
		Flux.never().distinct(null);
	}

	@Test(expected = NullPointerException.class)
	public void collectionSupplierNull() {
		new FluxDistinct<>(Flux.never(), k -> k, null);
	}

	@Test
	public void allDistinct() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allDistinctBackpressured() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allDistinctHide() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .hide()
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allDistinctBackpressuredHide() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .hide()
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void someDistinct() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1, 2, 2, 3, 4, 5, 6, 1, 2, 7, 7, 8, 9, 9, 10, 10, 10)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void someDistinctBackpressured() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1, 2, 2, 3, 4, 5, 6, 1, 2, 7, 7, 8, 9, 9, 10, 10, 10)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allSame() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1, 1, 1, 1, 1, 1, 1, 1, 1)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allSameFusable() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(Fuseable.ANY);

		Flux.just(1, 1, 1, 1, 1, 1, 1, 1, 1)
		    .distinct(k -> k)
		    .filter(t -> true)
		    .map(t -> t)
		    .cache(4)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertFuseableSource()
		  .assertFusionEnabled()
		  .assertFusionMode(Fuseable.ASYNC)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allSameBackpressured() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1, 1, 1, 1, 1, 1, 1, 1, 1)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void withKeyExtractorSame() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).distinct(k -> k % 3).subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void withKeyExtractorBackpressured() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).distinct(k -> k % 3).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void keyExtractorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).distinct(k -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void collectionSupplierThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		new FluxDistinct<>(Flux.range(1, 10), k -> k, () -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void collectionSupplierReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		new FluxDistinct<>(Flux.range(1, 10), k -> k, () -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	//see https://github.com/reactor/reactor-core/issues/577
	public void collectionSupplierLimitedFifo() {
		Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5, 6, 1, 3, 4, 1, 1, 1, 1, 2);

		StepVerifier.create(flux.distinct(Flux.identityFunction(), () -> new NaiveFifoQueue<>(5)))
	                .expectNext(1, 2, 3, 4, 5, 6, 1, 2)
	                .verifyComplete();

		StepVerifier.create(flux.distinct(Flux.identityFunction(),
						() -> new NaiveFifoQueue<>(3)))
	                .expectNext(1, 2, 3, 4, 5, 6, 1, 3, 4, 2)
	                .verifyComplete();
	}

	private static final class NaiveFifoQueue<T> extends AbstractCollection<T> {
		final int limit;
		int size = 0;
		Object[] array;

		public NaiveFifoQueue(int limit) {
			this.limit = limit;
			this.array = new Object[limit];
		}

		@Override
		@Nullable
		public Iterator<T> iterator() {
			return null;
		}

		@Override
		public void clear() {
			this.array = new Object[limit];
			this.size = 0;
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public boolean add(T t) {
			Objects.requireNonNull(t);
			for (int i = 0; i < array.length; i++) {
				if (array[i] == null) {
					array[i] = t;
					size++;
					return true;
				} else if (t.equals(array[i])) {
					return false;
				}
			}
			//at this point, no available slot and not a duplicate, drop oldest
			Object[] old = array;
			array = new Object[limit];
			System.arraycopy(old, 1, array, 0, array.length - 1);
			array[array.length - 1] = t;
			//size doesn't change
			return true;
		}
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxDistinct.DistinctSubscriber<String, Integer, Set<Integer>> test =
				new FluxDistinct.DistinctSubscriber<>(actual, new HashSet<>(), String::hashCode);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanConditionalSubscriber() {
		@SuppressWarnings("unchecked")
		Fuseable.ConditionalSubscriber<String> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
		FluxDistinct.DistinctConditionalSubscriber<String, Integer, Set<Integer>> test =
				new FluxDistinct.DistinctConditionalSubscriber<>(actual, new HashSet<>(), String::hashCode);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanFuseableSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxDistinct.DistinctFuseableSubscriber<String, Integer, Set<Integer>> test =
				new FluxDistinct.DistinctFuseableSubscriber<>(actual, new HashSet<>(), String::hashCode);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void distinctDefaulWithHashcodeCollisions() {
		Object foo = new Object() {
			@Override
			public int hashCode() {
				return 1;
			}
		};
		Object bar = new Object() {
			@Override
			public int hashCode() {
				return 1;
			}
		};

		assertThat(foo).isNotEqualTo(bar)
		               .hasSameHashCodeAs(bar);

		StepVerifier.create(Flux.just(foo, bar).distinct())
		            .expectNext(foo, bar)
		            .verifyComplete();
	}

	static class Foo {

		static final AtomicLong finalized = new AtomicLong();
		private final int i;

		public Foo(int i) {
			this.i = i;
		}

		@Override
		public int hashCode() {
			return i % 3;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof Foo && ((Foo) obj).i == i;
		}

		@Override
		protected void finalize() throws Throwable {
			finalized.incrementAndGet();
		}
	}

	@Test
	public void distinctDefaultDoesntRetainObjects() throws InterruptedException {
		Foo.finalized.set(0);
		Flux<Foo> test = Flux.range(1, 100)
		                     .map(i -> new Foo(i))
		                     .distinct();

		StepVerifier.create(test)
		            .expectNextCount(100)
		            .verifyComplete();

		System.gc();
		Thread.sleep(500);

		assertThat(Foo.finalized.longValue())
				.as("none retained")
				.isEqualTo(100);
	}

	@Test
	public void distinctDefaultErrorDoesntRetainObjects() throws InterruptedException {
		Foo.finalized.set(0);
		Flux<Foo> test = Flux.range(1, 100)
		                     .map(i -> new Foo(i))
		                     .concatWith(Mono.error(new IllegalStateException("boom")))
		                     .distinct();

		StepVerifier.create(test)
		            .expectNextCount(100)
		            .verifyErrorMessage("boom");

		System.gc();
		Thread.sleep(500);

		assertThat(Foo.finalized.longValue())
				.as("none retained after error")
				.isEqualTo(100);
	}

	@Test
	public void distinctDefaultCancelDoesntRetainObjects() throws InterruptedException {
		Foo.finalized.set(0);
		Flux<Foo> test = Flux.range(1, 100)
		                     .map(i -> new Foo(i))
		                     .concatWith(Mono.error(new IllegalStateException("boom")))
		                     .distinct()
		                     .take(50);

		StepVerifier.create(test)
		            .expectNextCount(50)
		            .verifyComplete();

		System.gc();
		Thread.sleep(500);

		assertThat(Foo.finalized.longValue())
				.as("none retained after cancel")
				.isEqualTo(50);
	}
}
