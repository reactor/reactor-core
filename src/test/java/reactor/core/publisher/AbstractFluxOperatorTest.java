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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Fuseable.ASYNC;

public abstract class AbstractFluxOperatorTest<I, O> {

	public interface Scenario<I, O> {

		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario) {
			return from(scenario, Fuseable.NONE);
		}

		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode) {
			return from(scenario, fusionMode, null);
		}

		@SuppressWarnings("unchecked")
		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode,
				Consumer<StepVerifier.Step<O>> verifier) {
			return from(scenario, fusionMode, null,
					verifier);
		}

		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode,
				Flux<I> finiteSource,
				Consumer<StepVerifier.Step<O>> verifier) {
			return new SimpleScenario<>(scenario, fusionMode, finiteSource, verifier);
		}

		Function<Flux<I>, Flux<O>> body();

		int fusionMode();

		Consumer<StepVerifier.Step<O>> verifier();

		Flux<I> finiteSource();
	}

	static final class SimpleScenario<I, O> implements Scenario<I, O> {

		final Function<Flux<I>, Flux<O>>     scenario;
		final int                            fusionMode;
		final Consumer<StepVerifier.Step<O>> verifier;
		final Flux<I>                        finiteSource;

		SimpleScenario(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode,
				Flux<I> finiteSource,
				Consumer<StepVerifier.Step<O>> verifier) {
			this.scenario = scenario;
			this.fusionMode = fusionMode;
			this.verifier = verifier;
			this.finiteSource = finiteSource;
		}

		@Override
		public Function<Flux<I>, Flux<O>> body() {
			return scenario;
		}

		@Override
		public int fusionMode() {
			return fusionMode;
		}

		@Override
		public Consumer<StepVerifier.Step<O>> verifier() {
			return verifier;
		}

		@Override
		public Flux<I> finiteSource() {
			return finiteSource;
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public final void assertPrePostState() {
		for (Scenario<I, O> scenario : scenarios_touchAndAssertState()) {
			if (scenario == null) {
				continue;
			}

			Flux<O> f = Flux.<I>from(s -> {
				Trackable t = null;
				if (s instanceof Trackable) {
					t = (Trackable) s;
					assertThat(t.getError()).isNull();
					assertThat(t.isStarted()).isFalse();
					assertThat(t.isTerminated()).isFalse();
				}

				if (s instanceof Receiver) {
					assertThat(((Receiver) s).upstream()).isNull();
				}

				if (s instanceof Loopback) {
					assertThat(((Loopback) s).connectedInput()).isNotNull();
				}

				if (s instanceof Producer) {
					assertThat(((Producer) s).downstream()).isNotNull();
				}

				s.onSubscribe(Operators.emptySubscription());
				s.onSubscribe(Operators.emptySubscription()); //noop path
				if (t != null) {
					assertThat(t.isStarted()).isTrue();
				}
				s.onComplete();
				if (t != null) {
					assertThat(t.isStarted()).isFalse();
					assertThat(t.isTerminated()).isTrue();
				}
			}).as(scenario.body());

			f.subscribe();

			f.filter(d -> true)
			 .subscribe();

			AtomicReference<Trackable> ref = new AtomicReference<>();
			f = finiteSourceOrDefault(scenario)
			            .doOnSubscribe(s -> {
				            Object _s =
						            ((Producer) ((Producer) s).downstream()).downstream();
				            if (_s instanceof Trackable) {
					            Trackable t = (Trackable) _s;
					            ref.set(t);
					            assertThat(t.isStarted()).isFalse();
				            }
			            })
			            .as(scenario.body())
			            .doOnSubscribe(parent -> {
				            if (parent instanceof Trackable) {
					            Trackable t = (Trackable) parent;
					            assertThat(t.getError()).isNull();
					            assertThat(t.isStarted()).isTrue();
					            assertThat(t.isTerminated()).isFalse();
				            }

				            //noop path
				            if (parent instanceof Subscriber) {
					            ((Subscriber<I>) parent).onSubscribe(Operators.emptySubscription());
				            }

				            if (parent instanceof Receiver) {
					            assertThat(((Receiver) parent).upstream()).isNotNull();
				            }

				            if (parent instanceof Loopback) {
					            assertThat(((Loopback) parent).connectedInput()).isNotNull();
				            }

				            if (parent instanceof Producer) {
					            assertThat(((Producer) parent).downstream()).isNotNull();
				            }
			            })
			            .doOnComplete(() -> {
				            if (ref.get() != null) {
					            assertThat(ref.get()
					                          .isStarted()).isFalse();
					            assertThat(ref.get()
					                          .isTerminated()).isTrue();
				            }
			            });

			f.subscribe();

			f.filter(t -> true)
			 .subscribe();

			resetHooks();
		}
	}

	@Test
	public final void errorWithUserProvidedCallback() {
		for (Scenario<I, O> scenario : scenarios_errorInOperatorCallback()) {
			if (scenario == null) {
				continue;
			}

			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			if (verifier == null) {
				String m = exception().getMessage();
				verifier = step -> step.verifyErrorMessage(m);
			}

			int fusion = scenario.fusionMode();

			verifier.accept(this.operatorNextVerifierBackpressured(scenario));
			verifier.accept(this.operatorNextVerifier(scenario));
			verifier.accept(this.operatorNextVerifierFused(scenario));

			if ((fusion & Fuseable.SYNC) != 0) {
				verifier.accept(this.operatorNextVerifierFusedSync(scenario));
				verifier.accept(this.operatorNextVerifierFusedConditionalSync(scenario));
			}

			if ((fusion & Fuseable.ASYNC) != 0) {
				verifier.accept(this.operatorNextVerifierFusedAsync(scenario));
				verifier.accept(this.operatorNextVerifierFusedConditionalAsync(scenario));
				this.operatorNextVerifierFusedAsyncState(scenario);
				this.operatorNextVerifierFusedConditionalAsyncState(scenario);
			}

			verifier.accept(this.operatorNextVerifierTryNext(scenario));
			verifier.accept(this.operatorNextVerifierBothConditional(scenario));
			verifier.accept(this.operatorNextVerifierConditionalTryNext(scenario));
			verifier.accept(this.operatorNextVerifierFusedTryNext(scenario));
			verifier.accept(this.operatorNextVerifierFusedBothConditional(scenario));
			verifier.accept(this.operatorNextVerifierFusedBothConditionalTryNext(scenario));

			resetHooks();
		}
	}

	@Test
	public final void errorWithUpstreamFailure() {
		for (Scenario<I, O> scenario : scenarios_errorFromUpstreamFailure()) {
			if (scenario == null) {
				continue;
			}

			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			if (verifier == null) {
				String m = exception().getMessage();
				verifier = step -> step.verifyErrorMessage(m);
			}

			verifier.accept(this.operatorErrorSourceVerifier(scenario));
			verifier.accept(this.operatorErrorSourceVerifierTryNext(scenario));
			verifier.accept(this.operatorErrorSourceVerifierFused(scenario));
			verifier.accept(this.operatorErrorSourceVerifierConditional(scenario));
			verifier.accept(this.operatorErrorSourceVerifierConditionalTryNext(scenario));
			verifier.accept(this.operatorErrorSourceVerifierFusedBothConditional(scenario));

			resetHooks();
		}
	}

	@Test
	public final void threeNextAndComplete() {
		for (Scenario<I, O> scenario : scenarios_threeNextAndComplete()) {
			if (scenario == null) {
				continue;
			}

			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			if (verifier == null) {
				verifier = defaultThreeNextExpectations(scenario);
				continue;
			}

			int fusion = scenario.fusionMode();

			verifier.accept(this.operatorNextVerifierBackpressured(scenario));
			verifier.accept(this.operatorNextVerifier(scenario));
			verifier.accept(this.operatorNextVerifierFused(scenario));

			if ((fusion & Fuseable.SYNC) != 0) {
				verifier.accept(this.operatorNextVerifierFusedSync(scenario));
				verifier.accept(this.operatorNextVerifierFusedConditionalSync(scenario));
			}

			if ((fusion & Fuseable.ASYNC) != 0) {
				verifier.accept(this.operatorNextVerifierFusedAsync(scenario));
				verifier.accept(this.operatorNextVerifierFusedConditionalAsync(scenario));
				this.operatorNextVerifierFusedAsyncState(scenario);
				this.operatorNextVerifierFusedConditionalAsyncState(scenario);
			}

			verifier.accept(this.operatorNextVerifierTryNext(scenario));
			verifier.accept(this.operatorNextVerifierBothConditional(scenario));
			verifier.accept(this.operatorNextVerifierConditionalTryNext(scenario));
			verifier.accept(this.operatorNextVerifierFusedTryNext(scenario));
			verifier.accept(this.operatorNextVerifierFusedBothConditional(scenario));
			verifier.accept(this.operatorNextVerifierFusedBothConditionalTryNext(scenario));

			resetHooks();
		}
	}

	@SuppressWarnings("unchecked") //default check identity
	protected Consumer<StepVerifier.Step<O>> defaultThreeNextExpectations(Scenario<I, O> scenario) {
		return step -> step.expectNext((O) multiItem(0))
		                   .expectNext((O) multiItem(1))
		                   .expectNext((O) multiItem(2))
		                   .verifyComplete();
	}

	//errorInOperatorCallbackVerification
	protected List<Scenario<I, O>> scenarios_errorInOperatorCallback() {
		return Collections.emptyList();
	}

	//errorInOperatorCallbackVerification
	protected List<Scenario<I, O>> scenarios_threeNextAndComplete() {
		return Collections.emptyList();
	}

	//assert
	protected List<Scenario<I, O>> scenarios_touchAndAssertState() {
		return scenarios_threeNextAndComplete();
	}

	//errorFromUpstreamFailureVerification
	protected List<Scenario<I, O>> scenarios_errorFromUpstreamFailure() {
		return scenarios_threeNextAndComplete();
	}

	//common source emitting
	protected void testPublisherSource(TestPublisher<I> ts) {
		ts.emit(multiItem(0), multiItem(1), multiItem(2));
	}

	//common fused source N prefilled
	protected void testUnicastSource(UnicastProcessor<I> ts) {
		ts.onNext(multiItem(0));
		ts.onNext(multiItem(1));
		ts.onNext(multiItem(2));
		ts.onComplete();
	}

	//common first item
	@SuppressWarnings("unchecked")
	protected I singleItem() {
		return (I) "test";
	}

	//common n unused item or dropped
	@SuppressWarnings("unchecked")
	protected I multiItem(int i) {
		if (i == 0) {
			return singleItem();
		}
		return (I) ("test" + i);
	}

	//common first unused item or dropped
	@SuppressWarnings("unchecked")
	protected I droppedItem() {
		return (I) "dropped";
	}

	//unprocessable exception (dropped)
	protected RuntimeException droppedException() {
		return new RuntimeException("dropped");
	}

	//exception
	protected RuntimeException exception() {
		return new RuntimeException("test");
	}

	protected Flux<I> finiteSourceOrDefault(Scenario<I, O> scenario) {
		Flux<I> source = scenario.finiteSource();
		if (source == null) {
			return Flux.just(multiItem(0), multiItem(1), multiItem(2));
		}
		return source;
	}

	final StepVerifier.Step<O> operatorNextVerifierBackpressured(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).hide()
		                                                          .as(scenario.body()),
				3);
	}

	final StepVerifier.Step<O> operatorNextVerifierTryNext(Scenario<I, O> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario.body()))
		                   .then(() -> testPublisherSource(ts));
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierTryNext(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();

		String dropped = droppedException().getMessage();
		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage(dropped);
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo(droppedItem());
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .as(scenario.body()))
		                   .then(() -> {
			                   ts.error(exception());

			                   //verify drop path
			                   ts.error(droppedException());
			                   ts.next(droppedItem());
			                   ts.complete();
			                   assertThat(errorDropped.get()).isTrue();
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifier(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean nextDropped = new AtomicBoolean();

		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo(droppedItem());
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .hide()
		                             .as(scenario.body()))
		                   .then(() -> {
			                   ts.error(exception());

			                   //verify drop path
			                   ts.next(droppedItem());
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorNextVerifierFused(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario)
		                                   .as(scenario.body()));
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedSync(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario)
		                                   .as(scenario.body()))
		                   .expectFusion(Fuseable.SYNC);
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedConditionalSync(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario)
		                                   .as(scenario.body())
		                                   .filter(d -> true))
		                   .expectFusion(Fuseable.SYNC);
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedAsync(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(up.as(scenario.body()))
		                   .expectFusion(Fuseable.ASYNC)
		                   .then(() -> testUnicastSource(up));
	}

	@SuppressWarnings("unchecked")
	final void operatorNextVerifierFusedAsyncState(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		testUnicastSource(up);
		StepVerifier.create(up.as(scenario.body()))
		            .consumeSubscriptionWith(s -> {
			            if (s instanceof Fuseable.QueueSubscription) {
				            Fuseable.QueueSubscription<O> qs =
						            ((Fuseable.QueueSubscription<O>) s);
				            qs.requestFusion(ASYNC);
				            assertThat(qs.size()).isEqualTo(3);
				            try {
				            	qs.poll();
				            	qs.poll();
				            }
				            catch (Exception e) {
				            }
				            if (qs instanceof Trackable && ((Trackable) qs).getError() != null) {
					            assertThat(((Trackable) qs).getError()).hasMessage(
							            exception().getMessage());
					            assertThat(qs.size()).isEqualTo(2);
				            }
				            qs.clear();
				            assertThat(qs.size()).isEqualTo(0);
			            }
		            })
		            .thenCancel()
		            .verify();
	}

	@SuppressWarnings("unchecked")
	final void operatorNextVerifierFusedConditionalAsyncState(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		testUnicastSource(up);
		StepVerifier.create(up.as(scenario.body())
		                      .filter(d -> true))
		            .consumeSubscriptionWith(s -> {
			            if (s instanceof Fuseable.QueueSubscription) {
				            Fuseable.QueueSubscription<O> qs =
						            ((Fuseable.QueueSubscription<O>) ((Receiver) s).upstream());
				            qs.requestFusion(ASYNC);
				            assertThat(qs.size()).isEqualTo(3);
				            try {
					            qs.poll();
					            qs.poll();
				            }
				            catch (Exception e) {
				            }
				            if (qs instanceof Trackable && ((Trackable) qs).getError() != null) {
					            assertThat(((Trackable) qs).getError()).hasMessage(
							            exception().getMessage());
					            assertThat(qs.size()).isEqualTo(2);
				            }
				            qs.clear();
				            assertThat(qs.size()).isEqualTo(0);
			            }
		            })
		            .thenCancel()
		            .verify();
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedConditionalAsync(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(up.as(scenario.body())
		                             .filter(d -> true))
		                   .expectFusion(Fuseable.ASYNC)
		                   .then(() -> testUnicastSource(up));
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> operatorErrorSourceVerifierFused(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();
		String dropped = droppedException().getMessage();
		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage(dropped);
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo(droppedItem());
			nextDropped.set(true);
		});
		return StepVerifier.create(up.as(scenario.body()))
		                   .then(() -> {
			                   up.actual.onError(exception());

			                   //verify drop path
			                   up.actual.onError(droppedException());
			                   assertThat(errorDropped.get()).isTrue();

			                   up.actual.onNext(droppedItem());
			                   assertThat(nextDropped.get()).isTrue();
			                   if (up.actual instanceof Fuseable.ConditionalSubscriber) {
				                   nextDropped.set(false);
				                   ((Fuseable.ConditionalSubscriber<I>) up.actual).tryOnNext(
						                   droppedItem());
				                   assertThat(nextDropped.get()).isTrue();
			                   }
			                   up.actual.onComplete();
		                   });
	}

	final StepVerifier.Step<O> operatorNextVerifierConditionalTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).hide()
		                                                          .as(scenario.body())
		                                                          .filter(filter -> true),
				3);
	}

	final StepVerifier.Step<O> operatorNextVerifierBothConditional(Scenario<I, O> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario.body())
		                             .filter(filter -> true))
		                   .then(() -> testPublisherSource(ts));
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedBothConditional(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body())
		                                                          .filter(filter -> true));
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierConditionalTryNext(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();

		String dropped = droppedException().getMessage();
		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage(dropped);
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo(droppedItem());
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .as(scenario.body())
		                             .filter(filter -> true))
		                   .then(() -> {
			                   ts.error(exception());

			                   //verify drop path
			                   ts.error(droppedException());
			                   ts.next(droppedItem());
			                   ts.complete();
			                   assertThat(errorDropped.get()).isTrue();
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierConditional(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean nextDropped = new AtomicBoolean();

		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo(droppedItem());
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .hide()
		                             .as(scenario.body())
		                             .filter(filter -> true))
		                   .then(() -> {
			                   ts.error(exception());

			                   //verify drop path
			                   ts.next(droppedItem());
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedBothConditionalTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body())
		                                                          .filter(filter -> true),
				3);
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> operatorErrorSourceVerifierFusedBothConditional(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();
		String dropped = droppedException().getMessage();
		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage(dropped);
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo(droppedItem());
			nextDropped.set(true);
		});
		return StepVerifier.create(up.as(scenario.body())
		                             .filter(filter -> true))
		                   .then(() -> {
			                   up.actual.onError(exception());

			                   //verify drop path
			                   up.actual.onError(droppedException());
			                   assertThat(errorDropped.get()).isTrue();

			                   up.actual.onNext(droppedItem());
			                   assertThat(nextDropped.get()).isTrue();

			                   if (up.actual instanceof Fuseable.ConditionalSubscriber) {
				                   nextDropped.set(false);
				                   ((Fuseable.ConditionalSubscriber<I>) up.actual).tryOnNext(
						                   droppedItem());
				                   assertThat(nextDropped.get()).isTrue();
			                   }
			                   up.actual.onComplete();
		                   });
	}

	final StepVerifier.Step<O> operatorNextVerifier(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).hide()
		                                                          .as(scenario.body()));
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body()),
				3);
	}


	@After
	public void resetHooks() {
		Hooks.resetOnErrorDropped();
		Hooks.resetOnNextDropped();
		Hooks.resetOnOperator();
		Hooks.resetOnOperatorError();
	}
}
