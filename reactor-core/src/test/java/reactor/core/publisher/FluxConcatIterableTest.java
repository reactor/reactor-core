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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxConcatIterableTest {

	@Test
	public void arrayNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.concat((Iterable<? extends Publisher<?>>) null);
		});
	}

	final Publisher<Integer> source = Flux.range(1, 3);

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concat(Arrays.asList(source, source, source)).subscribe(ts);

		ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.concat(Arrays.asList(source, source, source)).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(4);

		ts.assertValues(1, 2, 3, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oneSourceIsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concat(Arrays.asList(source, null, source)).subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void singleSourceIsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concat(Arrays.asList((Publisher<Integer>) null)).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void scanOperator(){
	    FluxConcatIterable<Integer> test = new FluxConcatIterable<>(Arrays.asList(source, source, source));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber(){
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		List<Publisher<Integer>> publishers = Arrays.asList(source, source, source);
		FluxConcatIterable.ConcatIterableSubscriber<Integer> test = new FluxConcatIterable.ConcatIterableSubscriber<>(actual, publishers.iterator());
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		test.missedRequested = 2;
		test.requested = 3;

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(5L);

	}

	static class TrackableIterable implements Iterable<Flux<String>> {
		private final List<Flux<String>> publishers;

		TrackableIterable() {
			this.publishers = Arrays.asList(
					Flux.just("A", "B"),
					Flux.just("C", "D"),
					Flux.just("E", "F")
			);
		}

		@Override
		public java.util.Iterator<Flux<String>> iterator() {
			return publishers.iterator();
		}
	}

	@Test
	void testIterableResourceIsReleasedWhenFluxIsReleased() throws InterruptedException {
		TrackableIterable trackableIterable = new TrackableIterable();
		WeakReference<TrackableIterable> resourceRef = new WeakReference<>(trackableIterable);

		Flux<String> concatenated = Flux.concat(trackableIterable);
		concatenated.blockLast();

		trackableIterable = null;
		concatenated = null;

		System.gc();
		Thread.sleep(100);

		assertThat(resourceRef.get()).isNull();
	}
}
