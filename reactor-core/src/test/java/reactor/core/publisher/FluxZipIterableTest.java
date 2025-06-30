/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxZipIterableTest extends FluxOperatorTest<String, String> {

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.zipWithIterable(() -> {
					throw exception();
				}, (a, b) -> a)),

				scenario(f -> f.zipWithIterable(Arrays.asList(1, 2, 3), (a, b) -> {
					throw exception();
				})),

				scenario(f -> f.zipWithIterable(() ->
						new Iterator<String>(){
							@Override
							public boolean hasNext() {
								throw exception();
							}

							@Override
							public String next() {
								return null;
							}
						}, (a, b) -> a)),

				scenario(f -> f.zipWithIterable(() ->
						new Iterator<String>(){
							@Override
							public boolean hasNext() {
								return true;
							}

							@Override
							public String next() {
								throw exception();
							}
						}, (a, b) -> a)),

				scenario(f -> f.zipWithIterable(() ->
						new Iterator<String>(){
							boolean invoked;
							@Override
							public boolean hasNext() {
								if(invoked){
									throw exception();
								}
								invoked = true;
								return true;
							}

							@Override
							public String next() {
								return item(0);
							}
						}, (a, b) -> a))
						.receiveValues(item(0))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.zipWithIterable(Arrays.asList(1, 2, 3), (a, b) -> a)),

				scenario(f -> f.zipWithIterable(Arrays.asList(1, 2, 3, 4, 5), (a, b) -> a))
		);
	}

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxZipIterable<>(null, Collections.emptyList(), (a, b) -> a);
		});
	}

	@Test
	public void iterableNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never()
					.zipWithIterable(null, (a, b) -> a);
		});
	}

	@Test
	public void zipperNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never()
					.zipWithIterable(Collections.emptyList(), null);
		});
	}

	@Test
	public void normalSameSize() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);

		ts.assertValues(11, 22, 33, 44, 55)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void normalSameSizeBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);

		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts.request(1);

		ts.assertValues(11)
		.assertNoError()
		.assertNotComplete();

		ts.request(2);

		ts.assertValues(11, 22, 33)
		.assertNoError()
		.assertNotComplete();

		ts.request(5);

		ts.assertValues(11, 22, 33, 44, 55)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void normalSourceShorter() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 4)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);

		ts.assertValues(11, 22, 33, 44)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void normalOtherShorter() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40), (a, b) -> a + b).subscribe(ts);

		ts.assertValues(11, 22, 33, 44)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void sourceEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 0)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40), (a, b) -> a + b).subscribe(ts);

		ts.assertNoValues()
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void otherEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				Collections.<Integer>emptyList(), (a, b) -> a + b).subscribe(ts);

		ts.assertNoValues()
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void zipperReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> (Integer)null).subscribe(ts);

		ts.assertNoValues()
		.assertNotComplete()
		.assertError(NullPointerException.class);
	}

	@Test
	public void iterableReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				() -> null, (a, b) -> a).subscribe(ts);

		ts.assertNoValues()
		.assertNotComplete()
		.assertError(NullPointerException.class);
	}

	@Test
	public void zipperThrowsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> { throw new RuntimeException("forced failure"); }).subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(RuntimeException.class)
				.assertErrorWith(e -> assertThat(e).hasMessageContaining("forced failure"));
	}

	@Test
	public void iterableThrowsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				() -> { throw new RuntimeException("forced failure"); }, (a, b) -> a).subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(RuntimeException.class)
				.assertErrorWith(e -> assertThat(e).hasMessageContaining("forced failure"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void zipWithIterable(){
		StepVerifier.create(Flux.just(0).zipWithIterable(Arrays.asList(1, 2, 3)))
	                .expectNext(Tuples.of(0, 1))
	                .verifyComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxZipIterable<Integer, Object, Integer> test = new FluxZipIterable<>(parent, Collections.emptyList(), (a, b) -> a);

		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanSingleSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxZipIterable.ZipSubscriber<Integer, Integer, Integer> test = new FluxZipIterable.ZipSubscriber<>(actual,
        		new ArrayList<Integer>().iterator(), (i, j) -> i + j);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

	private static class LargeResourceIterable implements Iterable<Integer> {
		private final byte[] largeBuffer = new byte[10 * 1024 * 1024];

		@Override
		public Iterator<Integer> iterator() {
			return new Iterator<Integer>() {
				private int count = 0;
				@Override
				public boolean hasNext() {
					return count < 2;
				}

				@Override
				public Integer next() {
					if (!hasNext()) throw new NoSuchElementException();
					return count++;
				}
			};
		}
	}

	@Test
	void testZipWithIterableResourceIsReleasedWhenFluxAndSubscriptionAreReleased() throws InterruptedException {
		AtomicReference<WeakReference<LargeResourceIterable>> resourceRef = new AtomicReference<>();
		AtomicReference<Subscription> subscriptionHolder = new AtomicReference<>();
		CountDownLatch completionLatch = new CountDownLatch(1);

		{
			LargeResourceIterable resourceIterable = new LargeResourceIterable();
			resourceRef.set(new WeakReference<>(resourceIterable));

			Flux<Tuple2<String, Integer>> fluxToZip = Flux.just("A", "B")
					.zipWithIterable(resourceIterable);

			fluxToZip.subscribe(new CoreSubscriber<Tuple2<String, Integer>>() {
				@Override
				public void onSubscribe(Subscription s) {
					subscriptionHolder.set(s);
					s.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(Tuple2<String, Integer> value) {
				}

				@Override
				public void onError(Throwable t) {
					completionLatch.countDown();
				}

				@Override
				public void onComplete() {
					completionLatch.countDown();
				}
			});
		}

		boolean completed = completionLatch.await(5, TimeUnit.SECONDS);
		assertThat(completed).isTrue();

		Subscription s = subscriptionHolder.get();
		if (s != null) {
			s.cancel();
		}
		subscriptionHolder.set(null);

		forceGc();

		assertThat(resourceRef.get().get()).isNull();
	}


	private void forceGc() throws InterruptedException {
		for (int i = 0; i < 5; i++) {
			System.gc();
			Thread.sleep(100);
		}
	}
}
