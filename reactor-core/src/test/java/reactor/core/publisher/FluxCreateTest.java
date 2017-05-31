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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import org.reactivestreams.Subscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.FluxCreate.BufferAsyncSink;
import reactor.core.publisher.FluxCreate.SerializedSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifier.Step;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class FluxCreateTest {

	@Test
	public void normalBuffered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux<Integer> source = Flux.<Signal<Integer>>create(e -> {
			e.next(Signal.next(1));
			e.next(Signal.next(2));
			e.next(Signal.next(3));
			e.next(Signal.complete());
			System.out.println(e.isCancelled());
			System.out.println(e.requestedFromDownstream());
		}).dematerialize();

		source.subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void fluxCreateBuffered() {
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

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(0);
	}

	@Test
	public void fluxCreateBuffered2() {
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

		assertThat(cancellation.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(0);
	}

	@Test
	public void fluxCreateBufferedError() {
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
	public void fluxCreateBufferedError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		});

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxCreateBufferedEmpty() {
		Flux<String> created = Flux.create(FluxSink::complete);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxCreateDisposables() {
		AtomicInteger dispose1 = new AtomicInteger();
		AtomicInteger dispose2 = new AtomicInteger();
		AtomicInteger cancel1 = new AtomicInteger();
		AtomicInteger cancel2 = new AtomicInteger();
		AtomicInteger cancellation = new AtomicInteger();
		Flux<String> created = Flux.create(s -> {
			s.onDispose(dispose1::getAndIncrement)
			 .onCancel(cancel1::getAndIncrement);
			s.onDispose(dispose2::getAndIncrement);
			assertThat(dispose2.get()).isEqualTo(1);
			s.onCancel(cancel2::getAndIncrement);
			assertThat(cancel2.get()).isEqualTo(1);
			s.onDispose(cancellation::getAndIncrement);
			assertThat(cancellation.get()).isEqualTo(1);
			assertThat(dispose1.get()).isEqualTo(0);
			assertThat(cancel1.get()).isEqualTo(0);
			s.next("test1");
			s.complete();
		});

		StepVerifier.create(created)
		            .expectNext("test1")
		            .verifyComplete();

		assertThat(dispose1.get()).isEqualTo(1);
		assertThat(cancel1.get()).isEqualTo(0);
	}

	@Test
	public void fluxCreateBufferedCancelled() {
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

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(1);
	}

	@Test
	public void fluxCreateBufferedBackpressured() {
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
	public void fluxPush() {
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
	public void fluxCreateSerialized() {
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
	public void fluxCreateSerialized2(){
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
	public void fluxCreateSerializedError() {
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
	public void fluxCreateSerializedError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		});

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxCreateSerializedEmpty() {
		Flux<String> created = Flux.create(s ->{
			s.complete();
		});

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxCreateSerializedCancelled() {
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

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(1);
	}

	@Test
	public void fluxCreateSerializedBackpressured() {
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
	public void fluxCreateSerializedConcurrent() {
		Scheduler.Worker w1 = Schedulers.elastic().createWorker();
		Scheduler.Worker w2 = Schedulers.elastic().createWorker();
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
				Assert.fail();
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
					            Assert.fail();
				            }
			            })
			            .assertNext(s -> {
			            	assertThat(ref.get()).isEqualTo(Thread.currentThread());
				            assertThat(s).isEqualTo("test2");
			            })
			            .assertNext(s -> {
			            	assertThat(ref.get()).isEqualTo(Thread.currentThread());
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
	public void fluxCreateLatest() {
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
	public void fluxCreateLatest2(){
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
	public void fluxCreateLatestError() {
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
	public void fluxCreateLatestError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.LATEST);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxCreateLatestEmpty() {
		Flux<String> created = Flux.create(FluxSink::complete
				, FluxSink.OverflowStrategy.LATEST);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxCreateLatestCancelled() {
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

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(1);
	}

	@Test
	public void fluxCreateLatestBackpressured() {
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
	public void fluxCreateDrop() {
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
	public void fluxCreateDrop2(){
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
	public void fluxCreateDropError() {
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
	public void fluxCreateDropError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.DROP);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxCreateDropEmpty() {
		Flux<String> created = Flux.create(FluxSink::complete
				, FluxSink.OverflowStrategy.DROP);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxCreateDropCancelled() {
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

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(1);
	}

	@Test
	public void fluxCreateDropBackpressured() {
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
	public void fluxCreateError() {
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
	public void fluxCreateError2(){
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
	public void fluxCreateErrorError() {
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
	public void fluxCreateErrorError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.ERROR);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxCreateErrorEmpty() {
		Flux<String> created = Flux.create(FluxSink::complete
				, FluxSink.OverflowStrategy.ERROR);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxCreateErrorCancelled() {
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

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(1);
	}

	@Test
	public void fluxCreateErrorBackpressured() {
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
	public void fluxCreateIgnore() {
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
	public void fluxCreateIgnore2(){
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
	public void fluxCreateIgnoreError() {
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
	public void fluxCreateIgnoreError2() {
		Flux<String> created = Flux.create(s -> {
			s.error(new Exception("test"));
		}, FluxSink.OverflowStrategy.IGNORE);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxCreateIgnoreEmpty() {
		Flux<String> created = Flux.create(FluxSink::complete
				, FluxSink.OverflowStrategy.IGNORE);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxCreateIgnoreCancelled() {
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

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(1);
	}

	@Test
	public void fluxCreateIgnoreBackpressured() {
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
			Assert.fail();
		}
		catch (AssertionError error){
			assertThat(error).hasMessageContaining(
					"request overflow (expected production of at most 1; produced: 2; request overflown by signal: onNext(test2))"
			);
		}
	}

	@Test
	public void fluxPushOnRequest() {
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
		assertThat(onRequest.get()).isEqualTo(1);
	}

	@Test
	public void fluxCreateGenerateOnRequest() {
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
	public void fluxCreateOnRequestSingleThread() {
		for (OverflowStrategy overflowStrategy : OverflowStrategy.values()) {
			testFluxCreateOnRequestSingleThread(overflowStrategy);
		}
	}

	private void testFluxCreateOnRequestSingleThread(OverflowStrategy overflowStrategy) {
		RequestTrackingTestQueue queue = new RequestTrackingTestQueue();
		Flux<Integer> created = Flux.create(pushPullSink -> {
				assertThat(pushPullSink instanceof SerializedSink).isTrue();
				SerializedSink<Integer> s = (SerializedSink<Integer>)pushPullSink;
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
				assertThat(s1 instanceof SerializedSink).isTrue();
				assertThat(s.onDispose(() -> {}) instanceof SerializedSink).isTrue();
				assertThat(s.onCancel(() -> {}) instanceof SerializedSink).isTrue();
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
	public void fluxCreateOnRequestMultipleThreadsSlowProducer() {
		for (OverflowStrategy overflowStrategy : OverflowStrategy.values()) {
			testFluxCreateOnRequesMultipleThreads(overflowStrategy, true);
		}
	}

	@Test
	public void fluxCreateOnRequestMultipleThreadsFastProducer() {
		for (OverflowStrategy overflowStrategy : OverflowStrategy.values()) {
			testFluxCreateOnRequesMultipleThreads(overflowStrategy, false);
		}
	}

	private void testFluxCreateOnRequesMultipleThreads(OverflowStrategy overflowStrategy, boolean slowProducer) {
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

		public void initialize(FluxSink<Integer> sink) {
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

		public void generateAsync(int requested, boolean slowProducer) {
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
	public void scanBaseSink() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxCreate.BaseSink<String> test = new FluxCreate.BaseSink<String>(actual) {
			@Override
			public FluxSink<String> next(String s) {
				return this;
			}
		};
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);
		test.request(100);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(100L);

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanBufferAsyncSink() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		BufferAsyncSink<String> test = new BufferAsyncSink<>(actual, 123);
		test.queue.offer("foo");

		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();

		test.error(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
	}

	@Test
	public void scanLatestAsyncSink() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxCreate.LatestAsyncSink<String> test = new FluxCreate.LatestAsyncSink<>(actual);

		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(0);
		test.queue.set("foo");
		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();

		test.error(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
	}

	@Test
	public void scanSerializedSink() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxCreate.BaseSink<String> decorated = new FluxCreate.LatestAsyncSink<>(actual);
		SerializedSink<String> test = new SerializedSink<>(decorated);

		test.queue.offer("foo");
		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);
		assertThat(decorated.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(0);

		decorated.request(100);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(100L);
		decorated.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();

		test.error = new IllegalStateException("boom");
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");

	}
}
