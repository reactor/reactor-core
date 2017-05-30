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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import static org.assertj.core.api.Assertions.assertThat;

public class FluxTakeTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxTake<>(null, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void numberIsInvalid() {
		Flux.never()
		    .take(-1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void numberIsInvalidFused() {
		Flux.just(1)
		    .take(-1);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .take(5)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .take(5)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeZero() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .take(0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeOverflowAttempt() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Publisher<Integer> p = s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext(1);
			s.onNext(2);
			try {
				s.onNext(3);
			}
			catch(RuntimeException re){
				Assert.assertTrue("cancelled", Exceptions.isCancel(re));
				return;
			}
			Assert.fail();
		};

		Flux.from(p)
		    .take(2)
		    .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void aFluxCanBeLimited(){
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(2)
		)
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void takeBackpressured() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					for(int i = 0 ; i < n; i ++) {
						s.onNext("test");
					}
				}

				boolean extraNext = true;

				@Override
				public void cancel() {
					if(extraNext){
						extraNext = false;
						s.onNext("test");
					}
				}
			});
		})
		                        .take(3), 0)
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("test", "test")
		            .thenRequest(1)
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
	public void takeFusedBackpressured() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.take(3), 0)
		            .expectFusion()
		            .then(() -> up.onNext("test"))
		            .then(() -> up.onNext("test2"))
		            .thenRequest(2)
		            .expectNext("test", "test2")
		            .then(() -> up.onNext("test3"))
		            .then(() -> up.onNext("test4"))
		            .thenRequest(1)
		            .expectNext("test3")
		            .thenRequest(1)
		            .verifyComplete();
	}

	@Test
	public void takeFusedBackpressuredCancelled() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.take(3).doOnSubscribe(s -> {
			assertThat(((Fuseable.QueueSubscription)s).size()).isEqualTo(0);
		}), 0)
		            .expectFusion()
		            .then(() -> up.onNext("test"))
		            .then(() -> up.onNext("test"))
		            .then(() -> up.onNext("test"))
		            .thenRequest(2)
		            .expectNext("test", "test")
		            .thenCancel()
					.verify();
	}


	@Test
	public void takeBackpressuredConditional() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					for(int i = 0 ; i < n; i ++) {
						s.onNext("test");
					}
				}

				boolean extraNext = true;

				@Override
				public void cancel() {
					if(extraNext){
						extraNext = false;
						s.onNext("test");
					}
				}
			});
		})
		                        .take(3)
								.filter("test"::equals), 0)
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("test", "test")
		            .thenRequest(1)
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void takeBackpressuredSourceConditional() {
		StepVerifier.create(Flux.from(_s -> {
			Fuseable.ConditionalSubscriber s = (Fuseable.ConditionalSubscriber)_s;

			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					for(int i = 0 ; i < n; i ++) {
						s.tryOnNext("test");
					}
				}

				boolean extraNext = true;

				@Override
				public void cancel() {
					if(extraNext){
						extraNext = false;
						s.tryOnNext("test");
					}
				}
			});
		})
		                        .take(3)
								.filter("test"::equals), 0)
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("test", "test")
		            .thenRequest(1)
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
	public void failNextIfTerminatedTake() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo(1));
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
			s.onNext(1);
		})
		                        .take(2))
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	public void failNextIfTerminatedTakeFused() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo(1));
		StepVerifier.create(up.take(2))
		            .then(() -> up.actual.onComplete())
		            .then(() -> up.actual.onNext(1))
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	public void failNextIfTerminatedTakeConditional() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo(1));
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
			s.onNext(1);
		})
		                        .take(2)
		                        .filter("test2"::equals))
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test // fixme when we have a fuseable testPublisher or an improved hide operator
	@SuppressWarnings("unchecked")
	public void failNextIfTerminatedTakeSourceConditional() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo(1));
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
			((Fuseable.ConditionalSubscriber)s).tryOnNext(1);
		})
		                        .take(2)
		                        .filter("test2"::equals))
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	public void take() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .take(2))
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void takeCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .take(3), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}

	@Test
	public void takeFused() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(2))
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void takeFusedSync() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(2))
		            .expectFusion(Fuseable.SYNC)
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void takeFusedAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.take(2))
		            .expectFusion(Fuseable.ASYNC)
		            .then(() -> {
			            up.onNext("test");
			            up.onNext("test2");
		            })
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void takeFusedCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(3), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}


	@Test
	public void takeConditional() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .take(2)
		                        .filter("test2"::equals))
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	public void takeConditionalCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .take(3)
		                        .filter("test2"::equals), 2)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void takeConditionalFused() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(2)
		                        .filter("test2"::equals))
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	public void takeConditionalFusedCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(3)
		                        .filter("test2"::equals), 2)
		            .expectNext("test2")
		            .thenCancel()
		            .verify();
	}

	@SuppressWarnings("unchecked")
	void assertTrackableBeforeOnSubscribe(InnerOperator t){
		assertThat(t.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
	}

	void assertTrackableAfterOnSubscribe(InnerOperator t){
		assertThat(t.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
	}

	void assertTrackableAfterOnComplete(InnerOperator t){
		assertThat(t.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void failDoubleError() {
		Hooks.onErrorDropped(e -> assertThat(e).hasMessage("test2"));
		StepVerifier.create(Flux.from(s -> {
			assertTrackableBeforeOnSubscribe((InnerOperator)s);
			s.onSubscribe(Operators.emptySubscription());
			assertTrackableAfterOnSubscribe((InnerOperator)s);
			s.onError(new Exception("test"));
			assertTrackableAfterOnComplete((InnerOperator)s);
			s.onError(new Exception("test2"));
		})
		                        .take(2))
		            .verifyErrorMessage("test");

		Hooks.resetOnErrorDropped();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void failConditionalDoubleError() {
		Hooks.onErrorDropped(e -> assertThat(e).hasMessage("test2"));
		StepVerifier.create(Flux.from(s -> {
			assertTrackableBeforeOnSubscribe((InnerOperator)s);
			s.onSubscribe(Operators.emptySubscription());
			assertTrackableAfterOnSubscribe((InnerOperator)s);
			s.onError(new Exception("test"));
			assertTrackableAfterOnComplete((InnerOperator)s);
			s.onError(new Exception("test2"));
		})
		                        .take(2).filter(d -> true))
		            .verifyErrorMessage("test");

		Hooks.resetOnErrorDropped();
	}


	@Test
	@SuppressWarnings("unchecked")
	public void failFusedDoubleError() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		Hooks.onErrorDropped(e -> assertThat(e).hasMessage("test2"));
		StepVerifier.create(up
		                        .take(2))
		            .consumeSubscriptionWith(s -> {
			            assertTrackableBeforeOnSubscribe((InnerOperator)s);
		            })
		            .then(() -> {
			            assertTrackableAfterOnSubscribe((InnerOperator)up.actual);
			            up.actual.onError(new Exception("test"));
			            assertTrackableAfterOnComplete((InnerOperator)up.actual);
			            up.actual.onError(new Exception("test2"));
		            })
		            .verifyErrorMessage("test");

		Hooks.resetOnErrorDropped();
	}

	@Test
	public void ignoreFusedDoubleComplete() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		StepVerifier.create(up
		                        .take(2).filter(d -> true))
		            .consumeSubscriptionWith(s -> {
			            assertTrackableAfterOnSubscribe((InnerOperator)s);
		            })
		            .then(() -> {
			            assertTrackableAfterOnSubscribe((InnerOperator)up.actual);
			            up.actual.onComplete();
			            assertTrackableAfterOnComplete((InnerOperator)up.actual);
			            up.actual.onComplete();
		            })
		            .verifyComplete();
	}

	@Test
	public void ignoreDoubleComplete() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
			s.onComplete();
		})
		                        .take(2))
		            .verifyComplete();
	}

	@Test
	public void assertPrefetch() {
		assertThat(Flux.just("test", "test2", "test3")
		               .hide()
		               .take(2)
		               .getPrefetch()).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void ignoreDoubleOnSubscribe() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
		})
		                        .take(2))
		            .verifyComplete();
	}
	@Test
	public void ignoreConditionalDoubleOnSubscribe() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
		})
		                        .take(2)
		                        .filter(d -> true))
		            .verifyComplete();
	}

	@Test
	public void takeZeroLongMaxWhenNoRequest() {
		TestPublisher<Integer> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .take(0), 0)
		            .thenAwait()
		            .then(() -> ts.assertMinRequested(Long.MAX_VALUE))
		            .then(() -> ts.complete())
		            .verifyComplete();
	}

	@Test
	public void takeZeroPassRequestWhenActive() {
		TestPublisher<Integer> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .take(0), 3)
		            .thenAwait()
		            .then(() -> ts.assertMinRequested(3))
		            .then(() -> ts.complete())
		            .verifyComplete();
	}

	@Test
	public void takeConditionalZeroLongMaxWhenNoRequest() {
		TestPublisher<Integer> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .take(0)
		                      .filter(d -> true), 0)
		            .thenAwait()
		            .then(() -> ts.assertMinRequested(Long.MAX_VALUE))
		            .then(() -> ts.complete())
		            .verifyComplete();
	}

	@Test
	public void takeConditionalZeroPassRequestWhenActive() {
		TestPublisher<Integer> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .take(0)
		                      .filter(d -> true), 3)
		            .thenAwait()
		            .then(() -> ts.assertMinRequested(3))
		            .then(() -> ts.complete())
		            .verifyComplete();
	}

	@Test
    public void scanSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxTake.TakeSubscriber<Integer> test = new FluxTake.TakeSubscriber<>(actual, 5);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }

	@Test
    public void scanConditionalSubscriber() {
		Fuseable.ConditionalSubscriber<Integer> actual = Mockito.mock(Fuseable.ConditionalSubscriber.class);
		FluxTake.TakeConditionalSubscriber<Integer> test = new FluxTake.TakeConditionalSubscriber<>(actual, 5);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }

    @Test
    public void scanFuseableSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxTake.TakeFuseableSubscriber<Integer> test = new FluxTake.TakeFuseableSubscriber<>(actual, 10);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }
}
