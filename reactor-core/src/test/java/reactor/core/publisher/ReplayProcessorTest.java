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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.assertj.core.api.Assertions;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.util.LoggerUtils;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.TestLogger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

// This is ok as this class tests the deprecated ReplayProcessor. Will be removed with it in 3.5.
@SuppressWarnings("deprecation")
public class ReplayProcessorTest {

	@BeforeEach
	public void virtualTime() {
		VirtualTimeScheduler.getOrSet();
	}

	@AfterEach
	public void teardownVirtualTime() {
		VirtualTimeScheduler.reset();
	}

	@Test
	public void currentSubscriberCount() {
		Sinks.Many<Integer> sink = ReplayProcessor.create();

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asFlux().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();

		sink.asFlux().subscribe();

		assertThat(sink.currentSubscriberCount()).isEqualTo(2);
	}

    @Test
    public void unbounded() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, true);

	    AssertSubscriber<Integer> ts = AssertSubscriber.create(0L);

	    rp.subscribe(ts);
        
        rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValues(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void bounded() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);

	    AssertSubscriber<Integer> ts = AssertSubscriber.create(0L);

	    rp.subscribe(ts);
        
        rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValues(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void cancel() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);

	    AssertSubscriber<Integer> ts = AssertSubscriber.create();

	    rp.subscribe(ts);
        
        ts.cancel();

	    assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();
    }

    @Test
    public void unboundedAfter() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, true);

	    AssertSubscriber<Integer> ts = AssertSubscriber.create(0L);

	    rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        rp.subscribe(ts);

	    assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValues(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void boundedAfter() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);

	    AssertSubscriber<Integer> ts = AssertSubscriber.create(0L);

	    rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        rp.subscribe(ts);

	    assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValues(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void unboundedLong() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, true);

	    AssertSubscriber<Integer> ts = AssertSubscriber.create(0L);

	    for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();

        rp.subscribe(ts);

	    assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();

        ts.assertNoValues();
        
        ts.request(Long.MAX_VALUE);
        
        ts.assertValueCount(256)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void boundedLong() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);
	    for (int i = 0; i < 256; i++) {
		    rp.onNext(i);
	    }
	    rp.onComplete();
	    StepVerifier.create(rp.hide())
	                .expectNextCount(16)
	                .verifyComplete();
    }

    @Test
    public void boundedLongError() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);
	    for (int i = 0; i < 256; i++) {
		    rp.onNext(i);
	    }
	    rp.onError(new Exception("test"));
	    StepVerifier.create(rp.hide())
	                .expectNextCount(16)
	                .verifyErrorMessage("test");
    }

    @Test
    public void unboundedFused() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, true);
	    for (int i = 0; i < 256; i++) {
		    rp.onNext(i);
	    }
	    rp.onComplete();
	    StepVerifier.create(rp)
	                .expectFusion(Fuseable.ASYNC)
	                .expectNextCount(256)
	                .verifyComplete();
    }

    @Test
    public void unboundedFusedError() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, true);
	    for (int i = 0; i < 256; i++) {
		    rp.onNext(i);
	    }
	    rp.onError(new Exception("test"));
	    StepVerifier.create(rp)
	                .expectFusion(Fuseable.ASYNC)
	                .expectNextCount(256)
	                .verifyErrorMessage("test");
    }

    @Test
    public void boundedFused() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);
	    for (int i = 0; i < 256; i++) {
		    rp.onNext(i);
	    }
	    rp.onComplete();
	    StepVerifier.create(rp)
	                .expectFusion(Fuseable.ASYNC)
	                .expectNextCount(256)
	                .verifyComplete();
    }

    @Test
    public void boundedFusedError() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);
	    for (int i = 0; i < 256; i++) {
		    rp.onNext(i);
	    }
	    rp.onError(new Exception("test"));
	    StepVerifier.create(rp)
	                .expectFusion(Fuseable.ASYNC)
	                .expectNextCount(16)
	                .verifyErrorMessage("test");
    }

    @Test
    public void boundedFusedAfter() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);

	    StepVerifier.create(rp)
	                .expectFusion(Fuseable.ASYNC)
	                .then(() -> {
		                for (int i = 0; i < 256; i++) {
			                rp.onNext(i);
		                }
		                rp.onComplete();
	                })
	                .expectNextCount(256)
	                .verifyComplete();
    }

	@Test
	public void timed() throws Exception {
		VirtualTimeScheduler.getOrSet();

		ReplayProcessor<Integer> rp =
				ReplayProcessor.createTimeout(Duration.ofSeconds(1));

		for (int i = 0; i < 5; i++) {
			rp.onNext(i);
		}

		VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

		for (int i = 5; i < 10; i++) {
			rp.onNext(i);
		}
		rp.onComplete();

		StepVerifier.create(rp.hide())
		            .expectFusion(Fuseable.NONE)
		            .expectNext(5,6,7,8,9)
		            .verifyComplete();
	}

	@Test
	public void timedError() throws Exception {
		VirtualTimeScheduler.getOrSet();

		ReplayProcessor<Integer> rp =
				ReplayProcessor.createTimeout(Duration.ofSeconds(1));

		for (int i = 0; i < 5; i++) {
			rp.onNext(i);
		}

		VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

		for (int i = 5; i < 10; i++) {
			rp.onNext(i);
		}
		rp.onError(new Exception("test"));

		StepVerifier.create(rp.hide())
		            .expectNext(5,6,7,8,9)
		            .verifyErrorMessage("test");
	}



	@Test
	public void timedAfter() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createTimeout(Duration.ofSeconds(1));

		StepVerifier.create(rp.hide())
		            .expectFusion(Fuseable.NONE)
		            .then(() -> {
			            for (int i = 0; i < 5; i++) {
				            rp.onNext(i);
			            }

			            VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

			            for (int i = 5; i < 10; i++) {
				            rp.onNext(i);
			            }
			            rp.onComplete();
		            })
		            .expectNext(0,1,2,3,4,5,6,7,8,9)
		            .verifyComplete();
	}

	@Test
	public void timedFused() throws Exception {
		VirtualTimeScheduler.getOrSet();

		ReplayProcessor<Integer> rp =
				ReplayProcessor.createTimeout(Duration.ofSeconds(1));


		for (int i = 0; i < 5; i++) {
			rp.onNext(i);
		}

		VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

		for (int i = 5; i < 10; i++) {
			rp.onNext(i);
		}
		rp.onComplete();

		StepVerifier.create(rp)
		            .expectFusion(Fuseable.NONE)
		            .expectNext(5,6,7,8,9)
		            .verifyComplete();
	}

	@Test
	public void timedFusedError() throws Exception {
		VirtualTimeScheduler.getOrSet();

		ReplayProcessor<Integer> rp =
				ReplayProcessor.createTimeout(Duration.ofSeconds(1));


		for (int i = 0; i < 5; i++) {
			rp.onNext(i);
		}

		VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

		for (int i = 5; i < 10; i++) {
			rp.onNext(i);
		}
		rp.onError(new Exception("test"));

		StepVerifier.create(rp)
		            .expectFusion(Fuseable.NONE)
		            .expectNext(5,6,7,8,9)
		            .verifyErrorMessage("test");
	}

	@Test
	public void timedFusedAfter() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createTimeout(Duration.ofSeconds(1));

		StepVerifier.create(rp)
		            .expectFusion(Fuseable.NONE)
		            .then(() -> {
			            for (int i = 0; i < 5; i++) {
				            rp.onNext(i);
			            }

			            VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

			            for (int i = 5; i < 10; i++) {
				            rp.onNext(i);
			            }
			            rp.onComplete();
		            })
		            .expectNext(0,1,2,3,4,5,6,7,8,9)
		            .verifyComplete();
	}

	@Test
	public void timedAndBound() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createSizeAndTimeout(5, Duration.ofSeconds(1));


		for (int i = 0; i < 10; i++) {
			rp.onNext(i);
		}

		VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

		for (int i = 10; i < 20; i++) {
			rp.onNext(i);
		}
		rp.onComplete();

		StepVerifier.create(rp.hide())
		            .expectFusion(Fuseable.NONE)
		            .expectNext(15,16,17,18,19)
		            .verifyComplete();

		assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();
    }

	@Test
	public void timedAndBoundError() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createSizeAndTimeout(5, Duration.ofSeconds(1));


		for (int i = 0; i < 10; i++) {
			rp.onNext(i);
		}

		VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

		for (int i = 10; i < 20; i++) {
			rp.onNext(i);
		}
		rp.onError(new Exception("test"));

		StepVerifier.create(rp.hide())
		            .expectFusion(Fuseable.NONE)
		            .expectNext(15,16,17,18,19)
		            .verifyErrorMessage("test");

		assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();
    }

	@Test
	public void timedAndBoundAfter() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createSizeAndTimeout(5, Duration.ofSeconds(1));

		StepVerifier.create(rp.hide())
		            .expectFusion(Fuseable.NONE)
		            .then(() -> {
			            for (int i = 0; i < 10; i++) {
				            rp.onNext(i);
			            }

			            VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

			            for (int i = 10; i < 20; i++) {
				            rp.onNext(i);
			            }
			            rp.onComplete();
		            })
		            .expectNextCount(20)
		            .verifyComplete();

		assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();
    }

	@Test
	public void timedAndBoundFused() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createSizeAndTimeout(5, Duration.ofSeconds(1));


		for (int i = 0; i < 10; i++) {
			rp.onNext(i);
		}

		VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

		for (int i = 10; i < 20; i++) {
			rp.onNext(i);
		}
		rp.onComplete();

		StepVerifier.create(rp)
		            .expectFusion(Fuseable.ASYNC)
		            .expectNext(15,16,17,18,19)
		            .verifyComplete();

		assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();
	}

	@Test
	public void timedAndBoundFusedError() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createSizeAndTimeout(5, Duration.ofSeconds(1));


		for (int i = 0; i < 10; i++) {
			rp.onNext(i);
		}

		VirtualTimeScheduler.get().advanceTimeBy(Duration.ofSeconds(2));

		for (int i = 10; i < 20; i++) {
			rp.onNext(i);
		}
		rp.onError(new Exception("test"));

		StepVerifier.create(rp)
		            .expectFusion(Fuseable.ASYNC)
		            .expectNext(15,16,17,18,19)
		            .verifyErrorMessage("test");

		assertThat(rp.currentSubscriberCount()).as("has subscriber").isZero();
	}

	@Test
	public void timedAndBoundedOnSubscribeAndState() {
		testReplayProcessorState(ReplayProcessor.createSizeAndTimeout(1, Duration.ofSeconds(1)));
	}

	@Test
	public void timedOnSubscribeAndState() {
		testReplayProcessorState(ReplayProcessor.createTimeout(Duration.ofSeconds(1)));
	}

	@Test
	public void unboundedOnSubscribeAndState() {
		testReplayProcessorState(ReplayProcessor.create(1, true));
	}

	@Test
	public void boundedOnSubscribeAndState() {
    	testReplayProcessorState(ReplayProcessor.cacheLast());
	}

	@SuppressWarnings("unchecked")
	void testReplayProcessorState(ReplayProcessor<String> rp) {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			Disposable d1 = rp.subscribe();

			rp.subscribe();

			ReplayProcessor.ReplayInner<String> s = ((ReplayProcessor.ReplayInner<String>) rp.inners()
			                                                                                 .findFirst()
			                                                                                 .get());

			assertThat(d1).isEqualTo(s.actual());

			assertThat(s.isEmpty()).isTrue();
			assertThat(s.isCancelled()).isFalse();
			assertThat(s.isCancelled()).isFalse();

			assertThat(rp.getPrefetch()).isEqualTo(Integer.MAX_VALUE);
			if (rp.getBufferSize() != Integer.MAX_VALUE) {
				assertThat(rp.getBufferSize()).isEqualTo(1);
			}
			FluxSink<String> sink = rp.sink();
			sink.next("test");
			rp.onComplete();

			rp.onComplete();

			Exception e = new RuntimeException("test");
			rp.onError(e);
			Assertions.assertThat(testLogger.getErrContent())
			          .contains("Operator called default onErrorDropped")
			          .contains(e.getMessage());
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void failNegativeBufferSizeBounded() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			ReplayProcessor.create(-1);
		});
	}

	@Test
	public void failNegativeBufferBoundedAndTimed() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			ReplayProcessor.createSizeAndTimeout(-1, Duration.ofSeconds(1));
		});
	}

	@Test
	public void scanProcessor() {
		ReplayProcessor<String> test = ReplayProcessor.create(16, false);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PARENT)).isEqualTo(subscription);

		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(16);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanProcessorUnboundedCapacity() {
		ReplayProcessor<String> test = ReplayProcessor.create(16, true);
		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void inners() {
		Sinks.Many<Integer> sink = ReplayProcessor.create(1);
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink.inners()).as("before subscriptions").isEmpty();

		sink.asFlux().subscribe(notScannable);
		sink.asFlux().subscribe(scannable);

		assertThat(sink.inners())
				.asList()
				.as("after subscriptions")
				.hasSize(2)
				.extracting(l -> (Object) ((ReplayProcessor.ReplayInner<?>) l).actual)
				.containsExactly(notScannable, scannable);
	}
}
