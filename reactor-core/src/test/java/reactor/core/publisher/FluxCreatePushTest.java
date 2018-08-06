/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import reactor.core.Exceptions;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class FluxCreatePushTest {

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
	public void contextTestPush() {
		StepVerifier.create(Flux.push(s -> IntStream.range(0, 10).forEach(i -> s.next(s
				.currentContext()
				.get(AtomicInteger.class)
				.incrementAndGet())))
		                        .take(10)
		                        .subscriberContext(ctx -> ctx.put(AtomicInteger.class,
				                        new AtomicInteger())))
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}
	
	
	//mirror tests in FluxCreate

	@Test
	public void normalBuffered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux<Integer> source = Flux.<Signal<Integer>>push(e -> {
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
	public void fluxPushBuffered() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.push(s -> {
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
	public void fluxPushBuffered2() {
		AtomicInteger cancellation = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		StepVerifier.create(Flux.push(s -> {
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
	public void fluxPushBufferedError() {
		Flux<String> created = Flux.push(s -> {
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
	public void fluxPushBufferedError2() {
		Flux<String> created = Flux.push(s -> {
			s.error(new Exception("test"));
		});

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxPushBufferedEmpty() {
		Flux<String> created = Flux.push(FluxSink::complete);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxPushDisposables() {
		AtomicInteger dispose1 = new AtomicInteger();
		AtomicInteger dispose2 = new AtomicInteger();
		AtomicInteger cancel1 = new AtomicInteger();
		AtomicInteger cancel2 = new AtomicInteger();
		AtomicInteger cancellation = new AtomicInteger();
		Flux<String> created = Flux.push(s -> {
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
	public void fluxPushBufferedCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.push(s -> {
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
	public void fluxPushBufferedBackpressured() {
		Flux<String> created = Flux.push(s -> {
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
	public void fluxPushSerialized() {
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
	public void fluxPushSerialized2(){
		StepVerifier.create(Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void fluxPushSerializedError() {
		Flux<String> created = Flux.push(s -> {
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
	public void fluxPushSerializedError2() {
		Flux<String> created = Flux.push(s -> {
			s.error(new Exception("test"));
		});

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxPushSerializedEmpty() {
		Flux<String> created = Flux.push(s ->{
			s.complete();
		});

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxPushSerializedCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.push(s -> {
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
	public void fluxPushSerializedBackpressured() {
		Flux<String> created = Flux.push(s -> {
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
	public void fluxPushLatest() {
		Flux<String> created = Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.LATEST);

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void fluxPushLatest2(){
		StepVerifier.create(Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.LATEST).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void fluxPushLatestError() {
		Flux<String> created = Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.error(new Exception("test"));
		}, OverflowStrategy.LATEST);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxPushLatestError2() {
		Flux<String> created = Flux.push(s -> {
			s.error(new Exception("test"));
		}, OverflowStrategy.LATEST);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxPushLatestEmpty() {
		Flux<String> created =
				Flux.push(FluxSink::complete, OverflowStrategy.LATEST);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxPushLatestCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.push(s -> {
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
		}, OverflowStrategy.LATEST);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .thenCancel()
		            .verify();

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(1);
	}

	@Test
	public void fluxPushLatestBackpressured() {
		Flux<String> created = Flux.push(s -> {
			assertThat(s.requestedFromDownstream()).isEqualTo(1);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.LATEST);

		StepVerifier.create(created, 1)
		            .expectNext("test1")
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("test3")
		            .verifyComplete();
	}

	@Test
	public void fluxPushDrop() {
		Flux<String> created = Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.DROP);

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void fluxPushDrop2(){
		StepVerifier.create(Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.DROP).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void fluxPushDropError() {
		Flux<String> created = Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.error(new Exception("test"));
		}, OverflowStrategy.DROP);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxPushDropError2() {
		Flux<String> created = Flux.push(s -> {
			s.error(new Exception("test"));
		}, OverflowStrategy.DROP);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxPushDropEmpty() {
		Flux<String> created =
				Flux.push(FluxSink::complete, OverflowStrategy.DROP);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxPushDropCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.push(s -> {
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
		}, OverflowStrategy.DROP);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .thenCancel()
		            .verify();

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(1);
	}

	@Test
	public void fluxPushDropBackpressured() {
		Flux<String> created = Flux.push(s -> {
			assertThat(s.requestedFromDownstream()).isEqualTo(1);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.DROP);

		StepVerifier.create(created, 1)
		            .expectNext("test1")
		            .thenAwait()
		            .thenRequest(2)
		            .verifyComplete();
	}

	@Test
	public void fluxPushError() {
		Flux<String> created = Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.ERROR);

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void fluxPushError2(){
		StepVerifier.create(Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.ERROR).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void fluxPushErrorError() {
		Flux<String> created = Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.error(new Exception("test"));
		}, OverflowStrategy.ERROR);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxPushErrorError2() {
		Flux<String> created = Flux.push(s -> {
			s.error(new Exception("test"));
		}, OverflowStrategy.ERROR);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxPushErrorEmpty() {
		Flux<String> created =
				Flux.push(FluxSink::complete, OverflowStrategy.ERROR);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxPushErrorCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.push(s -> {
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
		}, OverflowStrategy.ERROR);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .thenCancel()
		            .verify();

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(1);
	}

	@Test
	public void fluxPushErrorBackpressured() {
		Flux<String> created = Flux.push(s -> {
			assertThat(s.requestedFromDownstream()).isEqualTo(1);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.ERROR);

		StepVerifier.create(created, 1)
		            .expectNext("test1")
		            .thenAwait()
		            .thenRequest(2)
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void fluxPushIgnore() {
		Flux<String> created = Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.IGNORE);

		assertThat(created.getPrefetch()).isEqualTo(-1);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void fluxPushIgnore2(){
		StepVerifier.create(Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.IGNORE).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void fluxPushIgnoreError() {
		Flux<String> created = Flux.push(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.error(new Exception("test"));
		}, OverflowStrategy.IGNORE);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxPushIgnoreError2() {
		Flux<String> created = Flux.push(s -> {
			s.error(new Exception("test"));
		}, OverflowStrategy.IGNORE);

		StepVerifier.create(created)
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxPushIgnoreEmpty() {
		Flux<String> created =
				Flux.push(FluxSink::complete, OverflowStrategy.IGNORE);

		StepVerifier.create(created)
		            .verifyComplete();
	}

	@Test
	public void fluxPushIgnoreCancelled() {
		AtomicInteger onDispose = new AtomicInteger();
		AtomicInteger onCancel = new AtomicInteger();
		Flux<String> created = Flux.push(s -> {
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
		}, OverflowStrategy.IGNORE);

		StepVerifier.create(created)
		            .expectNext("test1", "test2", "test3")
		            .thenCancel()
		            .verify();

		assertThat(onDispose.get()).isEqualTo(1);
		assertThat(onCancel.get()).isEqualTo(1);
	}

	@Test
	public void fluxPushIgnoreBackpressured() {
		Flux<String> created = Flux.push(s -> {
			assertThat(s.requestedFromDownstream()).isEqualTo(1);
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}, OverflowStrategy.IGNORE);

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
					"request overflow (expected production of at most 1; produced: 2; request overflown by signal: onNext(test2))");
		}
	}

	@Test
	@Parameters(source = OverflowStrategy.class)
	public void sinkToString(OverflowStrategy strategy) {
		StepVerifier.create(Flux.push(sink -> {
			if (sink instanceof FluxCreate.SerializedSink) {
				sink.error(new IllegalArgumentException("expected no SerializedSink"));
			}
			else {
				sink.next(sink.toString());
				sink.complete();
			}
		}, strategy))
		            .expectNext("FluxSink(" + strategy + ")")
		            .verifyComplete();
	}
}