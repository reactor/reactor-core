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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

// This is ok as this class tests the deprecated FluxProcessor. Will be removed with it in 3.5.
@SuppressWarnings("deprecation")
public class FluxProcessorTest {

	@Test
	@SuppressWarnings("unchecked")
	public void failNullSubscriber() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			FluxProcessor.wrap(UnicastProcessor.create(), UnicastProcessor.create())
					.subscribe((Subscriber) null);
		});
	}

	@Test
	public void failNullUpstream() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			FluxProcessor.wrap(null, UnicastProcessor.create());
		});
	}

	@Test
	public void failNullDownstream() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			FluxProcessor.wrap(UnicastProcessor.create(), null);
		});
	}

	@Test
	public void testCapacity(){
		assertThat(FluxProcessor.wrap(UnicastProcessor.create(), UnicastProcessor
				.create()).getBufferSize())
				.isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void normalBlackboxProcessor(){
		UnicastProcessor<Integer> upstream = UnicastProcessor.create();
		FluxProcessor<Integer, Integer> processor =
				FluxProcessor.wrap(upstream, upstream.map(i -> i + 1)
				                                     .filter(i -> i % 2 == 0));

		DelegateProcessor<Integer, Integer> delegateProcessor =
				(DelegateProcessor<Integer, Integer>)processor;

		delegateProcessor.parents().findFirst().ifPresent(s ->
				assertThat(s).isInstanceOf(FluxFilterFuseable.class));


		StepVerifier.create(processor)
		            .then(() -> Flux.just(1, 2, 3).subscribe(processor))
		            .expectNext(2, 4)
		            .verifyComplete();
	}

	@Test
	public void disconnectedBlackboxProcessor(){
		UnicastProcessor<Integer> upstream = UnicastProcessor.create();
		FluxProcessor<Integer, Integer> processor =
				FluxProcessor.wrap(upstream, Flux.just(1));

		StepVerifier.create(processor)
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void symmetricBlackboxProcessor(){
		UnicastProcessor<Integer> upstream = UnicastProcessor.create();
		FluxProcessor<Integer, Integer> processor =
				FluxProcessor.wrap(upstream, upstream);

		StepVerifier.create(processor)
	                .then(() -> Flux.just(1).subscribe(processor))
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void errorSymmetricBlackboxProcessor(){
		UnicastProcessor<Integer> upstream = UnicastProcessor.create();
		FluxProcessor<Integer, Integer> processor =
				FluxProcessor.wrap(upstream, upstream);

		StepVerifier.create(processor)
	                .then(() -> Flux.<Integer>error(new Exception("test")).subscribe(processor))
	                .verifyErrorMessage("test");
	}

	@Test
	public void testSubmitSession() throws Exception {
		FluxProcessor<Integer, Integer> processor = EmitterProcessor.create();
		AtomicInteger count = new AtomicInteger();
		CountDownLatch latch = new CountDownLatch(1);
		Scheduler scheduler = Schedulers.parallel();
		processor.publishOn(scheduler)
		         .delaySubscription(Duration.ofMillis(1000))
		         .limitRate(1)
		         .subscribe(d -> {
			         count.incrementAndGet();
			         latch.countDown();
		         });

		FluxSink<Integer> session = processor.sink();
		session.next(1);
		//System.out.println(emission);
		session.complete();

		latch.await(5, TimeUnit.SECONDS);
		assertThat(count.get()).as("latch").isEqualTo(1);
		scheduler.dispose();
	}

	@Test
	public void testEmitter() throws Throwable {
		FluxProcessor<Integer, Integer> processor = EmitterProcessor.create();

		int n = 100_000;
		int subs = 4;
		final CountDownLatch latch = new CountDownLatch((n + 1) * subs);
		Scheduler c = Schedulers.single();
		for (int i = 0; i < subs; i++) {
			processor.publishOn(c)
			         .limitRate(1)
			         .subscribe(d -> latch.countDown(), null, latch::countDown);
		}

		FluxSink<Integer> session = processor.sink();

		for (int i = 0; i < n; i++) {
			while (session.requestedFromDownstream() == 0) {
			}
			session.next(i);
		}
		session.complete();

		boolean waited = latch.await(5, TimeUnit.SECONDS);
		assertThat(waited).as("latch : %d", latch.getCount()).isTrue();
		c.dispose();
	}
	@Test
	public void testEmitter2() throws Throwable {
		FluxProcessor<Integer, Integer> processor = EmitterProcessor.create();

		int n = 100_000;
		int subs = 4;
		final CountDownLatch latch = new CountDownLatch((n + 1) * subs);
		Scheduler c = Schedulers.single();
		for (int i = 0; i < subs; i++) {
			processor.publishOn(c)
			         .doOnComplete(latch::countDown)
			         .doOnNext(d -> latch.countDown())
			         .subscribe();
		}

		FluxSink<Integer> session = processor.sink();

		for (int i = 0; i < n; i++) {
			while (session.requestedFromDownstream() == 0) {
			}
			session.next(i);
		}
		session.complete();

		boolean waited = latch.await(5, TimeUnit.SECONDS);
		assertThat(waited).as("latch : %d", latch.getCount()).isTrue();
		c.dispose();
	}

	@Test
	public void serializedConcurrent() {
		Scheduler.Worker w1 = Schedulers.boundedElastic().createWorker();
		Scheduler.Worker w2 = Schedulers.boundedElastic().createWorker();
		CountDownLatch latch = new CountDownLatch(1);
		CountDownLatch latch2 = new CountDownLatch(1);
		AtomicReference<Thread> ref = new AtomicReference<>();

		ref.set(Thread.currentThread());

		DirectProcessor<String> rp = DirectProcessor.create();
		FluxProcessor<String, String> serialized = rp.serialize();

		try {
			StepVerifier.create(serialized)
			            .then(() -> {
				            w1.schedule(() -> serialized.onNext("test1"));
				            try {
					            latch2.await();
				            }
				            catch (InterruptedException e) {
					            fail("Unexpected InterruptedException");
				            }
				            w2.schedule(() -> {
					            serialized.onNext("test2");
					            serialized.onNext("test3");
					            serialized.onComplete();
					            latch.countDown();
				            });
			            })
			            .assertNext(s -> {
				            AssertionsForClassTypes.assertThat(s).isEqualTo("test1");
				            AssertionsForClassTypes.assertThat(ref.get()).isNotEqualTo(Thread.currentThread());
				            ref.set(Thread.currentThread());
				            latch2.countDown();
				            try {
					            latch.await();
				            }
				            catch (InterruptedException e) {
								fail("Unexpected InterruptedException");
				            }
			            })
			            .assertNext(s -> {
				            assertThat(ref).hasValue(Thread.currentThread());
				            AssertionsForClassTypes.assertThat(s).isEqualTo("test2");
			            })
			            .assertNext(s -> {
				            assertThat(ref).hasValue(Thread.currentThread());
				            AssertionsForClassTypes.assertThat(s).isEqualTo("test3");
			            })
			            .verifyComplete();
		}
		finally {
			w1.dispose();
			w2.dispose();
		}
	}

	@Test
	public void scanProcessor() {
		FluxProcessor<String, String> test = DirectProcessor.<String>create().serialize();

		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(16);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}
}
