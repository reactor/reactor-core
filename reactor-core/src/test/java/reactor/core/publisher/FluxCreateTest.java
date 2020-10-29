/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.FluxCreate.BufferAsyncSink;
import reactor.core.publisher.FluxCreate.LatestAsyncSink;
import reactor.core.publisher.FluxCreate.SerializedFluxSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifier.Step;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class FluxCreateTest {

	@Test
	void normalBuffered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux<Integer> source = Flux.<Signal<Integer>>create(e -> {
			e.next(Signal.next(1));
			e.next(Signal.next(2));
			e.next(Signal.next(3));
			e.next(Signal.complete());
		}).dematerialize();

		source.subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	void gh613() {
		AtomicBoolean cancelled = new AtomicBoolean();
		AtomicBoolean completed = new AtomicBoolean();
		AtomicBoolean errored = new AtomicBoolean();
		AtomicInteger i = new AtomicInteger();

		Flux<Integer> source = Flux.create(e -> {
			e.next(1);
			cancelled.set(e.isCancelled());
			e.next(2);
			e.next(3);
			e.complete();

		});

		source.subscribe(new Subscriber<Integer>(){
			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(Integer integer) {
				i.incrementAndGet();
				throw new RuntimeException();
			}

			@Override
			public void onError(Throwable t) {
				errored.set(true);
			}

			@Override
			public void onComplete() {
				completed.set(true);
			}
		});

		assertThat(cancelled.get()).isTrue();
		assertThat(completed.get()).isFalse();
		assertThat(errored.get()).isFalse();
		assertThat(i).hasValue(1);
	}

	@Test
	void fluxCreateBuffered() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.create(s -> {
			s.onDispose(onDispose::getAndIncrement)
			 .onCancel(onCancel::getAndIncrement);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		});

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();

		assertThat(onDispose).hasValue(1);
		assertThat(onCancel).hasValue(0);
	}

	@Test
	void fluxCreateBuffered2() {
		AtomicInteger cancellation = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		StepVerifier.create(Flux.create(s -> {
			s.onDispose(cancellation::getAndIncrement);
			s.onCancel(onCancel::getAndIncrement);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();

		assertThat(cancellation).hasValue(1);
		assertThat(onCancel).hasValue(0);
	}

	@Test
	void fluxCreateBufferedError() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.error(new Exception("test"));
		});

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateBufferedError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		});

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateBufferedEmpty() {
		Flux<String> created = Flux.create(FluxSink::complete);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	void fluxCreateDisposables() {
		AtomicInteger dispose1 = new AtomicInteger();
		AtomicInteger dispose2 = new AtomicInteger();
		AtomicInteger cancel1 = new AtomicInteger();
		AtomicInteger cancel2 = new AtomicInteger();
		AtomicInteger cancellation = new AtomicInteger();
		Flux<String> created = Flux.create(s -> {
			s.onDispose(dispose1::getAndIncrement)
			 .onCancel(cancel1::getAndIncrement);
			s.onDispose(dispose2::getAndIncrement);
			assertThat(dispose2).hasValue(1);
			s.onCancel(cancel2::getAndIncrement);
			assertThat(cancel2).hasValue(1);
			s.onDispose(cancellation::getAndIncrement);
			assertThat(cancellation).hasValue(1);
			assertThat(dispose1).hasValue(0);
			assertThat(cancel1).hasValue(0);
			s.next("test1");
			s.complete();
		});

		StepVerifier.create(created)
		            .expectNext("test1")
		            .verifyComplete();

		assertThat(dispose1).hasValue(1);
		assertThat(cancel1).hasValue(0);
	}

	@Test
	void fluxCreateBufferedCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.create(s -> {
			s.onDispose(() -> {
				onDispose.getAndIncrement();
				assertThat(s.isCancelled()).isTrue();
			});
			s.onCancel(() -> {
				onCancel.getAndIncrement();
				assertThat(s.isCancelled()).isTrue();
			});
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		});

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .thenCancel()
		            .verify();

		assertThat(onDispose).hasValue(1);
		assertThat(onCancel).hasValue(1);
	}

	@Test
	void fluxCreateOnDispose() {
		int count = 5;
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		class Emitter {

			private final FluxSink<Integer> sink;

			private Emitter(FluxSink<Integer> sink) {
				this.sink = sink;
			}

			public void emit(long n) {
				for (int i = 0; i < n; i++)
					sink.next(i);
				sink.complete();
			}
		}
		Flux<Integer> flux1 = Flux.create(s -> {
			Emitter emitter = new Emitter(s);
			s.onDispose(() -> onDispose.incrementAndGet());
			s.onCancel(() -> onCancel.incrementAndGet());
			s.onRequest(emitter::emit);
		});
		StepVerifier.create(flux1, count)
		            .expectNextCount(count)
		            .expectComplete()
		            .verify();
		assertThat(onDispose).hasValue(1);
		assertThat(onCancel).hasValue(0);

		onDispose.set(0);
		onCancel.set(0);
		Flux<Integer> flux2 = Flux.create(s -> {
			Emitter emitter = new Emitter(s);
			s.onRequest(emitter::emit);
			s.onDispose(() -> onDispose.incrementAndGet());
			s.onCancel(() -> onCancel.incrementAndGet());
		});
		StepVerifier.create(flux2, count)
		            .expectNextCount(count)
		            .expectComplete()
		            .verify();
		assertThat(onDispose).hasValue(1);
		assertThat(onCancel).hasValue(0);
	}

	@Test
	void monoFirstCancelThenOnCancel() {
		AtomicInteger onCancel = new AtomicInteger();
		AtomicReference<FluxSink<Object>> sink = new AtomicReference<>();
		StepVerifier.create(Flux.create(sink::set))
				.thenAwait()
				.consumeSubscriptionWith(Subscription::cancel)
				.then(() -> sink.get().onCancel(onCancel::getAndIncrement))
				.thenCancel()
				.verify();
		assertThat(onCancel).hasValue(1);
	}

	@Test
	void monoFirstCancelThenOnDispose() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicReference<FluxSink<Object>> sink = new AtomicReference<>();
		StepVerifier.create(Flux.create(sink::set))
				.thenAwait()
				.consumeSubscriptionWith(Subscription::cancel)
				.then(() -> sink.get().onDispose(onDispose::getAndIncrement))
				.thenCancel()
				.verify();
		assertThat(onDispose).hasValue(1);
	}

	@Test
	void fluxCreateBufferedBackpressured() {
		Flux<String> created = Flux.create(s -> {
			assertThat(s.requestedFromDownstream()).isEqualTo(1);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		});

		StepVerifier.create(created, 1)
		            .expectNext("test1")
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxPush() {
		Flux<String> created = Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		});

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateSerialized() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		});

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateSerialized2(){
		StepVerifier.create(Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateSerializedError() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.error(new Exception("test"));
		});

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateSerializedError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		});

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateSerializedEmpty() {
		Flux<String> created = Flux.create(s ->{
			s.complete();
		});

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	void fluxCreateSerializedCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.create(s -> {
			s.onDispose(onDispose::getAndIncrement)
			 .onCancel(onCancel::getAndIncrement);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			assertThat(s.isCancelled()).isTrue();
			s.complete();
		});

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .thenCancel()
		            .verify();

		assertThat(onDispose).hasValue(1);
		assertThat(onCancel).hasValue(1);
	}

	@Test
	void fluxCreateSerializedBackpressured() {
		Flux<String> created = Flux.create(s -> {
			assertThat(s.requestedFromDownstream()).isEqualTo(1);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		});

		StepVerifier.create(created, 1)
		            .expectNext("test1")
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateSerializedConcurrent() {
		Scheduler.Worker w1 = Schedulers.boundedElastic().createWorker();
		Scheduler.Worker w2 = Schedulers.boundedElastic().createWorker();
		CountDownLatch latch = new CountDownLatch(1);
		CountDownLatch latch2 = new CountDownLatch(1);
		AtomicReference<Thread> ref = new AtomicReference<>();

		ref.set(Thread.currentThread());

		Flux<String> created = Flux.create(serialized -> {
			w1.schedule(() -> serialized.next("test1"));
			try {
				latch2.await();
			}
			catch (InterruptedException e) {
				fail("unexpected InterruptedException");
			}
			w2.schedule(() -> {
				serialized.next("test2");
				serialized.next("test3");
				serialized.complete();
				latch.countDown();
			});
		}, FluxSink.OverflowStrategy.IGNORE);

		try {
			StepVerifier.create(created)
			            .assertNext(s -> {
				            assertThat(s).isEqualTo("test1");
				            assertThat(ref.get()).isNotEqualTo(Thread.currentThread());
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
				            assertThat(s).isEqualTo("test2");
			            })
			            .assertNext(s -> {
				            assertThat(ref).hasValue(Thread.currentThread());
				            assertThat(s).isEqualTo("test3");
			            })
			            .verifyComplete();
		}
		finally {
			w1.dispose();
			w2.dispose();
		}
	}

	@Test
	void fluxCreateLatest() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.LATEST);

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateLatest2(){
		StepVerifier.create(Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.LATEST).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateLatestError() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.LATEST);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateLatestError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.LATEST);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateLatestEmpty() {
		Flux<String> created =
				Flux.create(FluxSink::complete, FluxSink.OverflowStrategy.LATEST);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	void fluxCreateLatestCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.create(s -> {
			s.onDispose(() -> {
				onDispose.getAndIncrement();
				assertThat(s.isCancelled()).isTrue();
			});
			s.onCancel(() -> {
				onCancel.getAndIncrement();
				assertThat(s.isCancelled()).isTrue();
			});
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.LATEST);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .thenCancel()
		            .verify();

		assertThat(onDispose).hasValue(1);
		assertThat(onCancel).hasValue(1);
	}

	@Test
	void fluxCreateLatestBackpressured() {
		Flux<String> created = Flux.create(s -> {
			assertThat(s.requestedFromDownstream()).isEqualTo(1);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.LATEST);

		StepVerifier.create(created, 1)
		            .expectNext("test1")
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateDrop() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.DROP);

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateDrop2(){
		StepVerifier.create(Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.DROP).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateDropError() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.DROP);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateDropError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.DROP);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateDropEmpty() {
		Flux<String> created =
				Flux.create(FluxSink::complete, FluxSink.OverflowStrategy.DROP);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	void fluxCreateDropCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.create(s -> {
			s.onDispose(() -> {
				onDispose.getAndIncrement();
				assertThat(s.isCancelled()).isTrue();
			});
			s.onCancel(() -> {
				onCancel.getAndIncrement();
				assertThat(s.isCancelled()).isTrue();
			});
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.DROP);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .thenCancel()
		            .verify();

		assertThat(onDispose).hasValue(1);
		assertThat(onCancel).hasValue(1);
	}

	@Test
	void fluxCreateDropBackpressured() {
		Flux<String> created = Flux.create(s -> {
			assertThat(s.requestedFromDownstream()).isEqualTo(1);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.DROP);

		StepVerifier.create(created, 1)
		            .expectNext("test1")
		            .thenAwait()
		            .thenRequest(2)
		            .verifyComplete();
	}

	@Test
	void fluxCreateError() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.ERROR);

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateError2(){
		StepVerifier.create(Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.ERROR).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateErrorError() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.ERROR);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateErrorError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.ERROR);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateErrorEmpty() {
		Flux<String> created =
				Flux.create(FluxSink::complete, FluxSink.OverflowStrategy.ERROR);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	void fluxCreateErrorCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.create(s -> {
			s.onDispose(() -> {
				onDispose.getAndIncrement();
				assertThat(s.isCancelled()).isTrue();
			});
			s.onCancel(() -> {
				onCancel.getAndIncrement();
				assertThat(s.isCancelled()).isTrue();
			});
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.ERROR);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .thenCancel()
		            .verify();

		assertThat(onDispose).hasValue(1);
		assertThat(onCancel).hasValue(1);
	}

	@Test
	void fluxCreateErrorBackpressured() {
		Flux<String> created = Flux.create(s -> {
			assertThat(s.requestedFromDownstream()).isEqualTo(1);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.ERROR);

		StepVerifier.create(created, 1)
		            .expectNext("test1")
		            .thenAwait()
		            .thenRequest(2)
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	void fluxCreateIgnore() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.IGNORE);

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateIgnore2(){
		StepVerifier.create(Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.IGNORE).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	void fluxCreateIgnoreError() {
		Flux<String> created = Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.IGNORE);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateIgnoreError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.IGNORE);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	void fluxCreateIgnoreEmpty() {
		Flux<String> created =
				Flux.create(FluxSink::complete, FluxSink.OverflowStrategy.IGNORE);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	void fluxCreateIgnoreCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.create(s -> {
			s.onDispose(() -> {
				onDispose.getAndIncrement();
				assertThat(s.isCancelled()).isTrue();
			});
			s.onCancel(() -> {
				onCancel.getAndIncrement();
				assertThat(s.isCancelled()).isTrue();
			});
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.IGNORE);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .thenCancel()
		            .verify();

		assertThat(onDispose).hasValue(1);
		assertThat(onCancel).hasValue(1);
	}

	@Test
	void fluxCreateIgnoreBackpressured() {
		Flux<String> created = Flux.create(s -> {
			assertThat(s.requestedFromDownstream()).isEqualTo(1);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, FluxSink.OverflowStrategy.IGNORE);

		try {
			StepVerifier.create(created, 1)
			            .expectNext("test1")
			            .thenAwait()
			            .thenRequest(2)
			            .verifyComplete();
			fail("Expected AssertionError here");
		}
		catch (AssertionError error){
			assertThat(error).hasMessageContaining(
					"request overflow (expected production of at most 1; produced: 2; request overflown by signal: onNext(test2))");
		}
	}

	@Test
	void fluxPushOnRequest() {
		AtomicInteger index = new AtomicInteger(1);
		AtomicInteger onRequest = new AtomicInteger();
		Flux<Integer> created = Flux.push(s -> {
			s.onRequest(n -> {
				onRequest.incrementAndGet();
				assertThat(n).isEqualTo(Long.MAX_VALUE);
				for (int i = 0; i < 5; i++) {
					s.next(index.getAndIncrement());
				}
				s.complete();
			});
		}, OverflowStrategy.BUFFER);

		StepVerifier.create(created, 0)
		            .expectSubscription()
		            .thenAwait()
		            .thenRequest(1)
		            .expectNext(1)
		            .thenRequest(2)
		            .expectNext(2, 3)
		            .thenRequest(2)
		            .expectNext(4, 5)
		            .expectComplete()
		            .verify();
		assertThat(onRequest).hasValue(1);
	}

	@Test
	void fluxCreateGenerateOnRequest() {
		AtomicInteger index = new AtomicInteger(1);
		Flux<Integer> created = Flux.create(s -> {
			s.onRequest(n -> {
				for (int i = 0; i < n; i++) {
					s.next(index.getAndIncrement());
				}
			});
		});

		StepVerifier.create(created, 0)
		            .expectSubscription()
		            .thenAwait()
		            .thenRequest(1)
		            .expectNext(1)
		            .thenRequest(2)
		            .expectNext(2, 3)
		            .thenCancel()
		            .verify();
	}

	@Test
	void fluxCreateOnRequestSingleThread() {
		for (OverflowStrategy overflowStrategy : OverflowStrategy.values()) {
			testFluxCreateOnRequestSingleThread(overflowStrategy);
		}
	}

	private void testFluxCreateOnRequestSingleThread(OverflowStrategy overflowStrategy) {
		RequestTrackingTestQueue queue = new RequestTrackingTestQueue();
		Flux<Integer> created = Flux.create(pushPullSink -> {
			assertThat(pushPullSink).isInstanceOf(SerializedFluxSink.class);
			SerializedFluxSink<Integer> s = (SerializedFluxSink<Integer>)pushPullSink;
			FluxSink<Integer> s1 = s.onRequest(n -> {
				if (queue.sink == null) {
					queue.initialize(s);
					assertThat(n).isEqualTo(10);
				}

				queue.generate(5);
				queue.onRequest((int) n);
				if (s.sink instanceof BufferAsyncSink) {
					assertThat(((BufferAsyncSink<?>)s.sink).queue.size()).isEqualTo(0);
				}
				queue.pushToSink();
			});
			assertThat(s1).isInstanceOf(SerializedFluxSink.class);
			assertThat(s.onDispose(() -> {})).isInstanceOf(SerializedFluxSink.class);
			assertThat(s.onCancel(() -> {})).isInstanceOf(SerializedFluxSink.class);
		}, overflowStrategy);

		Step<Integer> step = StepVerifier.create(created, 0);
		for (int i = 0; i < 100; i++) {
			step = step.thenRequest(10)
			           .expectNextCount(5)
			           .then(() -> queue.generate(15))
			           .thenRequest(5)
			           .thenRequest(5)
			           .expectNextCount(15)
			           .thenAwait()
			           .thenRequest(25)
			           .then(() -> queue.generate(5))
			           .then(() -> queue.generate(5))
			           .expectNextCount(25)
			           .thenAwait();
		}
		step.thenCancel().verify();
		assertThat(queue.queue.isEmpty()).isTrue();
	}

	@Test
	void fluxCreateOnRequestMultipleThreadsSlowProducer() {
		for (OverflowStrategy overflowStrategy : OverflowStrategy.values()) {
			testFluxCreateOnRequestMultipleThreads(overflowStrategy, true);
		}
	}

	@Test
	void fluxCreateOnRequestMultipleThreadsFastProducer() {
		for (OverflowStrategy overflowStrategy : OverflowStrategy.values()) {
			testFluxCreateOnRequestMultipleThreads(overflowStrategy, false);
		}
	}

	private void testFluxCreateOnRequestMultipleThreads(OverflowStrategy overflowStrategy, boolean slowProducer) {
		int count = 10_000;
		TestQueue queue;
		if (overflowStrategy == OverflowStrategy.ERROR || overflowStrategy == OverflowStrategy.IGNORE)
			queue = new RequestTrackingTestQueue();
		else {
			queue = new TestQueue();
		}
		Flux<Integer> created = Flux.create(s -> {
			s.onRequest(n -> {
				if (queue.sink == null) {
					queue.initialize(s);
				}
				int r = n > count ? count : (int) n;
				queue.onRequest(r);
				queue.generateAsync(r, slowProducer);
			});
			s.onDispose(() -> queue.close());
		}, overflowStrategy);

		StepVerifier.create(created.take(count).publishOn(Schedulers.parallel(), 1000))
		            .expectNextCount(count)
		            .expectComplete()
		            .verify();
	}

	private static class TestQueue {

		protected ConcurrentLinkedQueue<Integer> queue;

		protected FluxSink<Integer> sink;

		private AtomicInteger index = new AtomicInteger();

		private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

		void initialize(FluxSink<Integer> sink) {
			this.queue = new ConcurrentLinkedQueue<>();
			this.sink = sink;
		}

		public void onRequest(int count) {
		}

		public void generate(int count) {
			for (int i = 0; i < count; i++)
				queue.offer(index.getAndIncrement());
			pushToSink();
		}

		void generateAsync(int requested, boolean slowProducer) {
			if (slowProducer) {
				for (int i = 0; i < 10; i++)
					executor.schedule(() -> generate(requested / 10), i, TimeUnit.MILLISECONDS);
				if (requested % 10 != 0)
					executor.schedule(() -> generate(requested % 10), 11, TimeUnit.MILLISECONDS);
			}
			else {
				executor.submit(() -> generate(requested * 2));
			}
			pushToSink();
		}

		public void pushToSink() {
			while (sink.requestedFromDownstream() > 0) {
				Integer item = queue.poll();
				if (item != null) {
					sink.next(item);
				} else {
					break;
				}
			}
		}

		public void close() {
			executor.shutdown();
		}
	}

	private static class RequestTrackingTestQueue extends TestQueue {

		private Semaphore pushSemaphore = new Semaphore(0);

		@Override
		public void onRequest(int count) {
			pushSemaphore.release(count);
		}

		@Override
		public void pushToSink() {
			while (pushSemaphore.tryAcquire()) {
				Integer item = queue.poll();
				if (item != null) {
					sink.next(item);
				} else {
					pushSemaphore.release();
					break;
				}
			}
		}
	}

	@Test
	public void scanOperator(){
		FluxCreate<?> test = new FluxCreate<>(v -> {}, OverflowStrategy.BUFFER, FluxCreate.CreateMode.PUSH_ONLY);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
	void scanBaseSink() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxCreate.BaseSink<String> test = new FluxCreate.BaseSink<String>(actual) {
			@Override
			public FluxSink<String> next(String s) {
				return this;
			}
		};
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);
		test.request(100);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(100L);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
	}

	@Test
	void scanBaseSinkTerminated() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxCreate.BaseSink<String> test = new FluxCreate.BaseSink<String>(actual) {
			@Override
			public FluxSink<String> next(String s) {
				return this;
			}
		};
		test.complete();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void scanBufferAsyncSink() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		BufferAsyncSink<String> test = new BufferAsyncSink<>(actual, 123);
		test.queue.offer("foo");

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		test.error(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
	}

	@Test
	void scanLatestAsyncSink() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		LatestAsyncSink<String> test = new LatestAsyncSink<>(actual);

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		test.queue.set("foo");
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		test.error(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
	}

	@Test
	void scanSerializedSink() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxCreate.BaseSink<String> decorated = new LatestAsyncSink<>(actual);
		SerializedFluxSink<String> test = new SerializedFluxSink<>(decorated);

		test.mpscQueue.offer("foo");
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
		assertThat(decorated.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);

		decorated.request(100);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(100L);
		decorated.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		test.error = new IllegalStateException("boom");
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");

	}

	@Test
	void contextTest() {
		StepVerifier.create(Flux.create(s -> IntStream.range(0, 10).forEach(i -> s.next(s
				.currentContext()
		                                                       .get(AtomicInteger.class)
		                                                       .incrementAndGet())))
		                        .take(10)
		                        .contextWrite(ctx -> ctx.put(AtomicInteger.class,
				                        new AtomicInteger())))
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	void contextTestPush() {
		StepVerifier.create(Flux.push(s -> IntStream.range(0, 10).forEach(i -> s.next(s
				.currentContext()
		                                                       .get(AtomicInteger.class)
		                                                       .incrementAndGet())))
		                        .take(10)
		                        .contextWrite(ctx -> ctx.put(AtomicInteger.class,
				                        new AtomicInteger())))
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	void bufferSinkToString() {
		StepVerifier.create(Flux.create(sink -> {
			sink.next(sink.toString());
			if (sink instanceof  SerializedFluxSink) {
				sink.next(((SerializedFluxSink) sink).sink.toString());
				sink.complete();
			}
			else {
				sink.error(new IllegalArgumentException("expected SerializedFluxSink"));
			}
		}, OverflowStrategy.BUFFER))
		            .expectNext("FluxSink(BUFFER)")
		            .expectNext("FluxSink(BUFFER)")
		            .verifyComplete();
	}

	@Test
	void dropSinkToString() {
		StepVerifier.create(Flux.create(sink -> {
			sink.next(sink.toString());
			if (sink instanceof  SerializedFluxSink) {
				sink.next(((SerializedFluxSink) sink).sink.toString());
				sink.complete();
			}
			else {
				sink.error(new IllegalArgumentException("expected SerializedFluxSink"));
			}
		}, OverflowStrategy.DROP))
		            .expectNext("FluxSink(DROP)")
		            .expectNext("FluxSink(DROP)")
		            .verifyComplete();
	}

	@Test
	void ignoreSinkToString() {
		StepVerifier.create(Flux.create(sink -> {
			sink.next(sink.toString());
			if (sink instanceof  SerializedFluxSink) {
				sink.next(((SerializedFluxSink) sink).sink.toString());
				sink.complete();
			}
			else {
				sink.error(new IllegalArgumentException("expected SerializedFluxSink"));
			}
		}, OverflowStrategy.IGNORE))
		            .expectNext("FluxSink(IGNORE)")
		            .expectNext("FluxSink(IGNORE)")
		            .verifyComplete();
	}

	@Test
	void errorSinkToString() {
		StepVerifier.create(Flux.create(sink -> {
			sink.next(sink.toString());
			if (sink instanceof  SerializedFluxSink) {
				sink.next(((SerializedFluxSink) sink).sink.toString());
				sink.complete();
			}
			else {
				sink.error(new IllegalArgumentException("expected SerializedFluxSink"));
			}
		}, OverflowStrategy.ERROR))
		            .expectNext("FluxSink(ERROR)")
		            .expectNext("FluxSink(ERROR)")
		            .verifyComplete();
	}

	@Test
	void latestSinkToString() {
		StepVerifier.create(Flux.create(sink -> {
			sink.next(sink.toString());
			if (sink instanceof  SerializedFluxSink) {
				sink.next(((SerializedFluxSink) sink).sink.toString());
				sink.complete();
			}
			else {
				sink.error(new IllegalArgumentException("expected SerializedFluxSink"));
			}
		}, OverflowStrategy.LATEST))
		            .expectNext("FluxSink(LATEST)")
		            .expectNext("FluxSink(LATEST)")
		            .verifyComplete();
	}

	@Test
	void bufferSinkRaceNextCancel() {
		AtomicInteger discarded = new AtomicInteger();
		final Context context = Operators.discardLocalAdapter(String.class, s -> discarded.incrementAndGet()).apply(Context.empty());

		BufferAsyncSink<String> sink = new BufferAsyncSink<>(new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				//do not request
			}

			@Override
			public Context currentContext() {
				return context;
			}
		}, 10);

		RaceTestUtils.race(sink::cancel,
				() -> sink.next("foo"));

		assertThat(sink.queue.poll()).as("internal queue empty").isNull();
		assertThat(discarded).as("discarded").hasValue(1);
	}

	@Test
	void bufferSinkRaceNextCancel_loop() {
		int failed = 0;
		for (int i = 0; i < 10_000; i++) {
			try {
				bufferSinkRaceNextCancel();
			}
			catch (AssertionError e) {
				failed++;
			}
		}
		assertThat(failed).as("failed").isZero();
	}

	@Test
	void latestSinkRaceNextCancel() {
		AtomicInteger discarded = new AtomicInteger();
		final Context context = Operators.discardLocalAdapter(String.class, s -> discarded.incrementAndGet()).apply(Context.empty());

		LatestAsyncSink<String> sink = new LatestAsyncSink<>(new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				//do not request
			}

			@Override
			public Context currentContext() {
				return context;
			}
		});

		RaceTestUtils.race(sink::cancel,
				() -> sink.next("foo"));

		assertThat(sink.queue).as("internal queue empty").hasValue(null);
		assertThat(discarded).as("discarded").hasValue(1);
	}

	@Test
	void latestSinkRaceNextCancel_loop() {
		int failed = 0;
		for (int i = 0; i < 10_000; i++) {
			try {
				latestSinkRaceNextCancel();
			}
			catch (AssertionError e) {
				failed++;
			}
		}
		assertThat(failed).as("failed").isZero();
	}

	@Test
	void serializedBufferSinkRaceNextCancel() {
		AtomicInteger discarded = new AtomicInteger();
		final Context context = Operators.discardLocalAdapter(String.class, s -> discarded.incrementAndGet()).apply(Context.empty());

		BufferAsyncSink<String> baseSink = new BufferAsyncSink<>(new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				//do not request
			}

			@Override
			public Context currentContext() {
				return context;
			}
		}, 10);
		SerializedFluxSink<String> serializedSink = new SerializedFluxSink<>(baseSink);

		RaceTestUtils.race(baseSink::cancel,
				() -> serializedSink.next("foo"));

		assertThat(serializedSink.mpscQueue.poll()).as("serialized internal queue empty").isNull();
		assertThat(baseSink.queue.poll()).as("bufferAsyncSink internal queue empty").isNull();
		assertThat(discarded).as("discarded").hasValue(1);
	}

	@Test
	void serializedBufferSinkRaceNextCancel_loop() {
		int failed = 0;
		for (int i = 0; i < 10_000; i++) {
			try {
				serializedBufferSinkRaceNextCancel();
			}
			catch (AssertionError e) {
				failed++;
			}
		}
		assertThat(failed).as("failed").isZero();
	}

	@ParameterizedTest
	@EnumSource(OverflowStrategy.class)
	void secondOnCancelHandlerIsDisposedImmediately(OverflowStrategy overflowStrategy) {
		AtomicInteger firstDisposed = new AtomicInteger();
		AtomicInteger secondDisposed = new AtomicInteger();
		Flux.create(sink -> sink.onCancel(firstDisposed::incrementAndGet)
		                        .onCancel(secondDisposed::incrementAndGet),
				overflowStrategy).subscribe();

		assertThat(firstDisposed).as("first handler for %s", overflowStrategy).hasValue(0);
		assertThat(secondDisposed).as("second handler for %s", overflowStrategy).hasValue(1);
	}

	@ParameterizedTest
	@EnumSource(OverflowStrategy.class)
	void secondOnDisposeHandlerIsDisposedImmediately(OverflowStrategy overflowStrategy) {
		AtomicInteger firstDisposed = new AtomicInteger();
		AtomicInteger secondDisposed = new AtomicInteger();
		Flux.create(sink -> sink.onDispose(firstDisposed::incrementAndGet)
		                        .onDispose(secondDisposed::incrementAndGet),
				overflowStrategy).subscribe();

		assertThat(firstDisposed).as("first handler for %s", overflowStrategy).hasValue(0);
		assertThat(secondDisposed).as("second handler for %s", overflowStrategy).hasValue(1);
	}
}
