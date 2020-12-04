/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;
import reactor.util.retry.Retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.with;

public class SerializedSubscriberTest {

	//see https://github.com/reactor/reactor-core/issues/2077
	@Test
	public void onNextRaceWithCancelDoesNotLeak() {
		int loops = 0;
		while (loops < 100_000) {
			CopyOnWriteArrayList<Object> discarded = new CopyOnWriteArrayList<>();
			AssertSubscriber<Integer> consumer = new AssertSubscriber<>(
					Operators.enableOnDiscard(Context.empty(), discarded::add),
					Long.MAX_VALUE);

			SerializedSubscriber<Integer> leaky = new SerializedSubscriber<>(consumer);
			leaky.onSubscribe(Operators.emptySubscription());

			//we use constant so that if debugging is needed we can identify which step caused an issue
			leaky.onNext(1);
			RaceTestUtils.race(
					() -> {
						leaky.onNext(2);
						leaky.onNext(4);
					},
					() -> {
						leaky.onNext(3);
						leaky.cancel();
					}
			);

			//since we want to test for the race condition, we're not interested in iterations where all gets consumed or all gets discarded
			//however, we'll smoke test that in these cases at least the other side (discarded or consumed) sees the total number of elements
			if (consumer.values().size() == 4) {
				assertThat(discarded).as("when consumed all, none discarded").isEmpty();
				continue;
			}
			if (discarded.size() == 4) {
				assertThat(consumer.values()).as("when discarded all, none consumed").isEmpty();
				continue;
			}

			//now the meat of the test is to check that total discarded + total seen by consumer = total produced
			assertThat(discarded.size() + consumer.values().size())
					.as("elements discarded or passed down in round #%s: <%s> and <%s>", loops, discarded, consumer.values())
					.isEqualTo(4);

			loops++;
		}
	}

	//direct transcription of test case exposed in https://github.com/reactor/reactor-core/issues/2077
	@Test
	public void testLeakWithRetryWhenImmediatelyCancelled() throws InterruptedException {
		AtomicInteger counter = new AtomicInteger();
		AtomicInteger discarded = new AtomicInteger();
		AtomicInteger seen = new AtomicInteger();

		final CountDownLatch latch = new CountDownLatch(4);
		Flux.<Integer>generate(s -> {
			int i = counter.incrementAndGet();
			if (i == 100_000) {
				s.next(i);
				s.complete();
			}
			else {
				s.next(i);
			}
		})
			.doFinally(sig -> latch.countDown())
		    .publishOn(Schedulers.single())
			.doFinally(sig -> latch.countDown())
		    .retryWhen(Retry.from(p -> p.take(3)))
			.doFinally(sig -> latch.countDown())
		    .cancelOn(Schedulers.parallel())
		    .doOnDiscard(Integer.class, i -> discarded.incrementAndGet())
		    .doFinally(sig -> latch.countDown())
            .subscribeWith(new BaseSubscriber<Integer>() {
	            @Override
	            protected void hookOnNext(Integer value) {
					seen.incrementAndGet();
		            cancel();
	            }
            });

		assertThat(latch.await(5, TimeUnit.SECONDS)).as("latch 5s").isTrue();
		with().pollInterval(50, TimeUnit.MILLISECONDS)
		      .await().atMost(500, TimeUnit.MILLISECONDS)
		      .untilAsserted(() -> {
				  int expectedCnt = counter.get();
				  int snn = seen.get();
				  int discrdd = discarded.get();
				  assertThat(expectedCnt)
					      .withFailMessage("counter not equal to seen+discarded: Expected <%s>, got <%s+%s>=<%s>",
							      expectedCnt, seen, discarded, snn + discrdd)
					      .isEqualTo(snn + discrdd);
		      });
	}

	@Test
	public void scanSerializedSubscriber() {
		LambdaSubscriber<String> actual = new LambdaSubscriber<>(null, e -> { }, null, null);
		SerializedSubscriber<String> test = new SerializedSubscriber<>(actual);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isZero();
		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(SerializedSubscriber.LinkedArrayNode.DEFAULT_CAPACITY);

		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanSerializedSubscriberMaxBuffered() {
		LambdaSubscriber<String> actual = new LambdaSubscriber<>(null, e -> { }, null, null);
		SerializedSubscriber<String> test = new SerializedSubscriber<>(actual);

		test.tail = new SerializedSubscriber.LinkedArrayNode<>("");
		test.tail.count = Integer.MAX_VALUE;

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.LARGE_BUFFERED)).isNull();
	}

}
