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
package reactor.core.publisher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class FluxCreateTest {

	@Test
	public void normalBuffered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux<Integer> source = Flux.<Signal<Integer>>create(e -> {
			e.serialize().next(Signal.next(1));
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
			s.setCancellation(cancellation::getAndIncrement);
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
			s.setCancellation(cancellation::getAndIncrement);
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
	public void fluxCreateSerialized() {
		Flux<String> created = Flux.create(s -> {
			s = s.serialize();
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
			s = s.serialize();
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
			s = s.serialize();
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
			s = s.serialize();
			s.error(new Exception("test"));
		});

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxCreateSerializedEmpty() {
		Flux<String> created = Flux.create(s ->{
			s = s.serialize();
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
			s = s.serialize();
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
			s = s.serialize();
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

		Flux<String> created = Flux.create(s -> {
			FluxSink<String> serialized = s.serialize();
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
}