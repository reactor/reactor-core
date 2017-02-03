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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.MultiProducer;
import reactor.core.MultiReceiver;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.concurrent.QueueSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Fuseable.*;
import static reactor.core.Trackable.UNSPECIFIED;

public abstract class AbstractFluxOperatorTest<I, O> {

	public final Scenario<I, O> scenario(Function<Flux<I>, Flux<O>> scenario) {
		return Scenario.create(scenario)
		               .applyAllOptions(defaultScenario);
	}

	static final class Scenario<I, O> {

		static <I, O> Scenario<I, O> create(Function<Flux<I>, Flux<O>> scenario) {
			return new Scenario<>(scenario, new Exception("scenario:"));
		}

		final Function<Flux<I>, Flux<O>> scenario;
		final Exception                  stack;

		int                            fusionMode                           = NONE;
		int                            prefetch                             = -1;
		boolean                        shouldHitDropNextHookAfterTerminate  = true;
		boolean                        shouldHitDropErrorHookAfterTerminate = true;
		boolean                        shouldAssertPostTerminateState       = true;
		Flux<I>                        finiteFlux                           = null;
		String                         description                          = null;
		Consumer<StepVerifier.Step<O>> verifier                             = null;

		Scenario(Function<Flux<I>, Flux<O>> scenario, Exception stack) {
			this.scenario = scenario;
			this.stack = stack;
		}

		public boolean shouldHitDropNextHookAfterTerminate() {
			return shouldHitDropNextHookAfterTerminate;
		}

		public Scenario<I, O> shouldHitDropNextHookAfterTerminate(boolean shouldHitDropNextHookAfterTerminate) {
			this.shouldHitDropNextHookAfterTerminate =
					shouldHitDropNextHookAfterTerminate;
			return this;
		}

		public boolean shouldHitDropErrorHookAfterTerminate() {
			return shouldHitDropErrorHookAfterTerminate;
		}

		public Scenario<I, O> shouldHitDropErrorHookAfterTerminate(boolean shouldHitDropErrorHookAfterTerminate) {
			this.shouldHitDropErrorHookAfterTerminate =
					shouldHitDropErrorHookAfterTerminate;
			return this;
		}

		public boolean shouldAssertPostTerminateState() {
			return shouldAssertPostTerminateState;
		}

		public Scenario<I, O> shouldAssertPostTerminateState(boolean shouldAssertPostTerminateState) {
			this.shouldAssertPostTerminateState = shouldAssertPostTerminateState;
			return this;
		}

		public int fusionMode() {
			return fusionMode;
		}

		public Scenario<I, O> fusionMode(int prefetch) {
			this.fusionMode = prefetch;
			return this;
		}

		public int prefetch() {
			return prefetch;
		}

		public Scenario<I, O> prefetch(int prefetch) {
			this.prefetch = prefetch;
			return this;
		}

		public Flux<I> finiteFlux() {
			return finiteFlux;
		}

		public Scenario<I, O> finiteFlux(Flux<I> finiteSource) {
			this.finiteFlux = finiteSource;
			return this;
		}

		public Flux<I> description() {
			return finiteFlux;
		}

		public Scenario<I, O> description(String description) {
			this.description = description;
			return this;
		}

		public Consumer<StepVerifier.Step<O>> verifier() {
			return verifier;
		}

		public Scenario<I, O> verifier(Consumer<StepVerifier.Step<O>> verifier) {
			this.verifier = verifier;
			return this;
		}

		public Function<Flux<I>, Flux<O>> body() {
			return scenario;
		}

		public Scenario<I, O> applyAllOptions(Scenario<I, O> source) {
			if(source == null){
				return this;
			}
			this.description = source.description;
			this.fusionMode = source.fusionMode;
			this.prefetch = source.prefetch;
			this.shouldHitDropNextHookAfterTerminate =
					source.shouldHitDropNextHookAfterTerminate;
			this.shouldHitDropErrorHookAfterTerminate =
					source.shouldHitDropErrorHookAfterTerminate;
			this.shouldAssertPostTerminateState = source.shouldAssertPostTerminateState;
			this.finiteFlux = source.finiteFlux;
			this.verifier = source.verifier;
			return this;
		}
	}

	Scenario<I, O> defaultScenario;

	@Test
	@SuppressWarnings("unchecked")
	public final void assertPrePostState() {
		for (Scenario<I, O> scenario : scenarios_touchAndAssertState()) {
			if (scenario == null) {
				continue;
			}

			try {

				Flux<O> f = Flux.<I>from(s -> {
					Trackable t = null;
					if (s instanceof Trackable) {
						t = (Trackable) s;
						assertThat(t.getError()).isNull();
						assertThat(t.isStarted()).isFalse();
						assertThat(t.isTerminated()).isFalse();
						assertThat(t.isCancelled()).isFalse();

						if (scenario.prefetch() != UNSPECIFIED) {
							assertThat(Math.min(Integer.MAX_VALUE,t.getCapacity()))
									.isEqualTo(scenario.prefetch());
							if (t.expectedFromUpstream() != UNSPECIFIED &&
									t.expectedFromUpstream() != t.limit()) {
								assertThat(t.expectedFromUpstream()).isEqualTo(0);
							}
							if (t.limit() != UNSPECIFIED) {
								assertThat(t.limit()).isEqualTo(defaultLimit(scenario));
							}
						}

						if (t.getPending() != UNSPECIFIED && scenario.shouldAssertPostTerminateState()) {
							assertThat(t.getPending()).isEqualTo(3);
						}
						if (t.requestedFromDownstream() != UNSPECIFIED && scenario.shouldAssertPostTerminateState()) {
							assertThat(t.requestedFromDownstream()).isEqualTo(0);
						}
					}

					if (s instanceof Receiver) {
						assertThat(((Receiver) s).upstream()).isNull();
					}

					touchTreeState(s);

					s.onSubscribe(Operators.emptySubscription());
					s.onSubscribe(Operators.emptySubscription()); //noop path
					s.onSubscribe(Operators.cancelledSubscription()); //noop path
					if (t != null) {

						assertThat(t.isStarted()).isTrue();
						if (scenario.prefetch() != UNSPECIFIED) {
							if (t.expectedFromUpstream() != UNSPECIFIED && t
									.expectedFromUpstream() != t.limit()) {
								assertThat(t.expectedFromUpstream()).isEqualTo(scenario.prefetch());
							}
						}
						if (t.requestedFromDownstream() != UNSPECIFIED) {
							assertThat(t.requestedFromDownstream()).isEqualTo(Long.MAX_VALUE);
						}
					}
					s.onComplete();
					if (t != null) {
						touchTreeState(s);
						if (scenario.shouldAssertPostTerminateState()) {
							assertThat(t.isStarted()).isFalse();
							assertThat(t.isTerminated()).isTrue();
						}
					}
				}).as(scenario.body());

				if (scenario.prefetch() != UNSPECIFIED) {
					assertThat(Math.min(f.getPrefetch(), Integer.MAX_VALUE)).isEqualTo
							(scenario.prefetch());
				}

				if (f instanceof Loopback) {
					assertThat(((Loopback) f).connectedInput()).isNotNull();
				}

				f.subscribe();

				f = f.filter(t -> true);

				if (scenario.prefetch() != UNSPECIFIED) {
					assertThat(Math.min(((Flux) (((Receiver) f).upstream()))
							.getPrefetch(), Integer.MAX_VALUE)).isEqualTo(scenario
							.prefetch());
				}

				if (((Receiver) f).upstream() instanceof Loopback) {
					assertThat(((Loopback) (((Receiver) f).upstream())).connectedInput()).isNotNull();
				}

				f.subscribe();

				AtomicReference<Trackable> ref = new AtomicReference<>();
				Flux<O> source = finiteSourceOrDefault(scenario).doOnSubscribe(s -> {
					if(s instanceof Producer) {
						Object _s = ((Producer) ((Producer) s).downstream()).downstream();
						if (_s instanceof Trackable) {
							Trackable t = (Trackable) _s;
							ref.set(t);
							assertThat(t.isStarted()).isFalse();
							assertThat(t.isCancelled()).isFalse();
							if (scenario.prefetch() != UNSPECIFIED) {
								assertThat(Math.min(Integer.MAX_VALUE,t.getCapacity()))
								               .isEqualTo
										(scenario.prefetch());
								if (t.expectedFromUpstream() != UNSPECIFIED && t.limit
										() != t.expectedFromUpstream()) {
									assertThat(t.expectedFromUpstream()).isEqualTo(0);
								}
								if (t.limit() != UNSPECIFIED) {
									assertThat(t.limit()).isEqualTo(defaultLimit(scenario));
								}
							}
							if (t.getPending() != UNSPECIFIED && scenario.shouldAssertPostTerminateState()) {
								assertThat(t.getPending()).isEqualTo(3);
							}
							if (t.requestedFromDownstream() != UNSPECIFIED && scenario.shouldAssertPostTerminateState()) {
								assertThat(t.requestedFromDownstream()).isEqualTo(0);
							}
							if (t.expectedFromUpstream() != UNSPECIFIED && scenario
									.shouldAssertPostTerminateState() && t.limit() != t
									.expectedFromUpstream()) {
								assertThat(t.expectedFromUpstream()).isEqualTo(0);
							}
						}
					}
				})
				                                                .as(scenario.body());

				if (source.getPrefetch() != UNSPECIFIED && scenario.prefetch() != UNSPECIFIED) {
					assertThat(Math.min(source.getPrefetch(), Integer.MAX_VALUE)).isEqualTo(scenario.prefetch());
				}

				f = source.doOnSubscribe(parent -> {
					if (parent instanceof Trackable) {
						Trackable t = (Trackable) parent;
						assertThat(t.getError()).isNull();
						assertThat(t.isStarted()).isTrue();
						if (scenario.prefetch() != UNSPECIFIED && t.expectedFromUpstream() != UNSPECIFIED) {
							assertThat(t.expectedFromUpstream()).isEqualTo(scenario.prefetch());
						}
						assertThat(t.isTerminated()).isFalse();
					}

					//noop path
					if (parent instanceof Subscriber) {
						((Subscriber<I>) parent).onSubscribe(Operators.emptySubscription());
						((Subscriber<I>) parent).onSubscribe(Operators.cancelledSubscription());
					}

					if (parent instanceof Receiver) {
						assertThat(((Receiver) parent).upstream()).isNotNull();
					}

					touchTreeState(parent);
				})
				          .doOnComplete(() -> {
					          if (ref.get() != null) {
						          Trackable t = ref.get();
						          if (t.requestedFromDownstream() != UNSPECIFIED) {
							          assertThat(t.requestedFromDownstream()).isEqualTo(
									          Long.MAX_VALUE);
						          }
						          if (scenario.shouldAssertPostTerminateState()) {
							          assertThat(t.isStarted()).isFalse();
							          assertThat(t.isTerminated()).isTrue();
						          }
						          touchTreeState(ref.get());
					          }
				          });

				f.subscribe(r -> touchTreeState(ref.get()));

				source = source.filter(t -> true);

				if (scenario.prefetch() != UNSPECIFIED) {
					assertThat(Math.min(((Flux) (((Receiver) source).upstream()))
							.getPrefetch(), Integer.MAX_VALUE)).isEqualTo(
							scenario.prefetch());
				}

				f = f.filter(t -> true);

				f.subscribe(r -> touchTreeState(ref.get()));
			}
			catch (Error | RuntimeException e){
				if (scenario.stack != null) {
					e.addSuppressed(scenario.stack);
				}
				throw e;
			}
			catch (Throwable e) {
				if (scenario.stack != null) {
					e.addSuppressed(scenario.stack);
				}
				throw Exceptions.bubble(e);
			}
			finally {
				resetHooks();
			}
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
				verifier = step -> {
					try {
						step.verifyErrorMessage(m);
//						step.expectErrorMessage(m)
//						.verifyThenAssertThat()
//						.hasOperatorErrorWithMessage(m);
					}
					catch (Exception e) {
						e.printStackTrace();
						assertThat(Exceptions.unwrap(e)).hasMessage(m);
					}
				};
			}

			int fusion = scenario.fusionMode();

			try {

				verifier.accept(this.operatorNextVerifierBackpressured(scenario));
				verifier.accept(this.operatorNextVerifier(scenario));
				verifier.accept(this.operatorNextVerifierFused(scenario));

				if ((fusion & Fuseable.SYNC) != 0) {
					verifier.accept(this.operatorNextVerifierFusedSync(scenario));
					verifier.accept(this.operatorNextVerifierFusedConditionalSync(scenario));
				}

				if ((fusion & Fuseable.ASYNC) != 0) {
					verifier.accept(this.operatorNextVerifierFusedAsync(scenario));
					verifier.accept(this.operatorNextVerifierFusedConditionalAsync(
							scenario));
					this.operatorNextVerifierFusedAsyncState(scenario);
					this.operatorNextVerifierFusedConditionalAsyncState(scenario);
				}

				verifier.accept(this.operatorNextVerifierTryNext(scenario));
				verifier.accept(this.operatorNextVerifierBothConditional(scenario));
				verifier.accept(this.operatorNextVerifierConditionalTryNext(scenario));
				verifier.accept(this.operatorNextVerifierFusedTryNext(scenario));
				verifier.accept(this.operatorNextVerifierFusedBothConditional(scenario));
				verifier.accept(this.operatorNextVerifierFusedBothConditionalTryNext(
						scenario));

			}
			catch (Error | RuntimeException e){
				if (scenario.stack != null) {
					e.addSuppressed(scenario.stack);
				}
				throw e;
			}
			catch (Throwable e) {
				if (scenario.stack != null) {
					e.addSuppressed(scenario.stack);
				}
				throw Exceptions.propagate(e);
			}
			finally {
				resetHooks();
			}
		}
	}

	@Test
	public final void errorWithUpstreamFailure() {
		for (Scenario<I, O> scenario : scenarios_errorFromUpstreamFailure()) {
			if (scenario == null) {
				continue;
			}

			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			int fusion = scenario.fusionMode();

			if (verifier == null) {
				String m = exception().getMessage();
				verifier = step -> {
					try {
						step.verifyErrorMessage(m);
					}
					catch (Exception e) {
						assertThat(Exceptions.unwrap(e)).hasMessage(m);
					}
				};
			}

			try {
				verifier.accept(this.operatorErrorSourceVerifier(scenario));
				verifier.accept(this.operatorErrorSourceVerifierTryNext(scenario));
				verifier.accept(this.operatorErrorSourceVerifierFused(scenario));
				verifier.accept(this.operatorErrorSourceVerifierConditional(scenario));
				verifier.accept(this.operatorErrorSourceVerifierConditionalTryNext(
						scenario));
				verifier.accept(this.operatorErrorSourceVerifierFusedBothConditional(
						scenario));

				if (scenario.prefetch() != UNSPECIFIED || (fusion & Fuseable.SYNC) != 0) {
					verifier.accept(this.operatorErrorSourceVerifierFusedSync(scenario));
				}
				if (scenario.prefetch() != UNSPECIFIED || (fusion & Fuseable.ASYNC) != 0) {
					verifier.accept(this.operatorErrorSourceVerifierFusedAsync(scenario));
				}

			}
			catch (Error | RuntimeException e){
				if (scenario.stack != null) {
					e.addSuppressed(scenario.stack);
				}
				throw e;
			}
			catch (Throwable e) {
				if (scenario.stack != null) {
					e.addSuppressed(scenario.stack);
				}
				throw Exceptions.bubble(e);
			}
			finally {
				resetHooks();
			}
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
			}

			int fusion = scenario.fusionMode();

			try {

				verifier.accept(this.operatorNextVerifierBackpressured(scenario));
				this.operatorNextVerifierBackpressured(scenario)
				    .thenCancel()
				    .verify();

				verifier.accept(this.operatorNextVerifier(scenario));
				this.operatorNextVerifier(scenario)
				    .consumeSubscriptionWith(Subscription::cancel)
				    .thenCancel()
				    .verify();

				verifier.accept(this.operatorNextVerifierFusedTryNext(scenario));
				verifier.accept(this.operatorNextVerifierFused(scenario));
				this.operatorNextVerifierFused(scenario)
				    .consumeSubscriptionWith(Subscription::cancel)
				    .thenCancel()
				    .verify();

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
				this.operatorNextVerifierBothConditionalCancel(scenario)
				    .consumeSubscriptionWith(Subscription::cancel)
				    .thenCancel() //hit double cancel
				    .verify();

				verifier.accept(this.operatorNextVerifierConditionalTryNext(scenario));

				verifier.accept(this.operatorNextVerifierFusedBothConditional(scenario));
				this.operatorNextVerifierFusedBothConditional(scenario)
				    .consumeSubscriptionWith(Subscription::cancel)
				    .thenCancel()
				    .verify();

				verifier.accept(this.operatorNextVerifierFusedBothConditionalTryNext(scenario));

			}
			catch (Error | RuntimeException e){
				if (scenario.stack != null) {
					e.addSuppressed(scenario.stack);
				}
				throw e;
			}
			catch (Throwable e) {
				if (scenario.stack != null) {
					e.addSuppressed(scenario.stack);
				}
				throw Exceptions.bubble(e);
			}
			finally {
				resetHooks();
			}
		}
	}

	protected int defaultLimit(Scenario<I, O> scenario){
		if(scenario.prefetch() == UNSPECIFIED){
			return QueueSupplier.SMALL_BUFFER_SIZE - (QueueSupplier.SMALL_BUFFER_SIZE >> 2);
		}
		if(scenario.prefetch() == Integer.MAX_VALUE){
			return Integer.MAX_VALUE;
		}
		return scenario.prefetch() - (scenario.prefetch() >> 2);
	}

	@SuppressWarnings("unchecked") //default check identity
	protected Consumer<StepVerifier.Step<O>> defaultThreeNextExpectations(Scenario<I, O> scenario) {
		return step -> step.expectNext((O) item(0))
		                   .expectNext((O) item(1))
		                   .expectNext((O) item(2))
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
	protected void testPublisherSource(Scenario<I, O> scenario, TestPublisher<I> ts) {
		finiteSourceOrDefault(scenario).subscribe(ts::next, ts::error, ts::complete);
	}

	//common fused source N prefilled
	protected void testUnicastSource(Scenario<I, O> scenario, UnicastProcessor<I> ts) {
		finiteSourceOrDefault(scenario).subscribe(ts);
	}

	protected int fusionModeThreadBarrierSupport() {
		return NONE;
	}

	//common n unused item or dropped
	@SuppressWarnings("unchecked")
	protected I item(int i) {
		if (i == 0) {
			return (I) "test";
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

	final Flux<I> finiteSourceOrDefault(Scenario<I, O> scenario) {
		Flux<I> source = scenario != null ? scenario.finiteFlux() : null;
		if (source == null) {
			return Flux.just(item(0), item(1), item(2));
		}
		return source;
	}

	final StepVerifier.Step<O> operatorNextVerifierBackpressured(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).hide()
		                                                          .as(scenario.body()),
				3)
				.consumeSubscriptionWith(s -> s.request(0)); //request validation path
	}

	final StepVerifier.Step<O> operatorNextVerifierTryNext(Scenario<I, O> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario.body()))
		                   .then(() -> testPublisherSource(scenario, ts));
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
			                   if (scenario.shouldHitDropErrorHookAfterTerminate()) {
				                   ts.complete();
				                   ts.error(droppedException());
				                   assertThat(errorDropped.get()).isTrue();
			                   }
			                   if (scenario.shouldHitDropNextHookAfterTerminate()) {
				                   ts.next(droppedItem());
				                   assertThat(nextDropped.get()).isTrue();
			                   }
		                   });
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifier(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation
						.CLEANUP_ON_TERMINATE, TestPublisher.Violation.REQUEST_OVERFLOW);
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
			                   if (scenario.shouldHitDropNextHookAfterTerminate()) {
				                   ts.next(droppedItem());
				                   assertThat(nextDropped.get()).isTrue();
			                   }
		                   });
	}

	final StepVerifier.Step<O> operatorNextVerifierFused(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario)
		                                   .as(scenario.body()));
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> operatorNextVerifierFusedSync(Scenario<I, O> scenario) {
		if ((scenario.fusionMode() & Fuseable.SYNC) != 0) {
			StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body()), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs =
							            ((Fuseable.QueueSubscription<O>) s);

					            assertThat(qs.requestFusion(Fuseable.SYNC | THREAD_BARRIER)).isEqualTo(
							            fusionModeThreadBarrierSupport() & Fuseable.SYNC);

					            qs.size();
					            qs.isEmpty();
					            qs.clear();
					            assertThat(qs.isEmpty()).isTrue();
				            }
			            })
			            .thenCancel()
			            .verify();

			StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body()), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs =
							            ((Fuseable.QueueSubscription<O>) s);
					            assertThat(qs.requestFusion(NONE)).isEqualTo(NONE);
				            }
			            })
			            .thenCancel()
			            .verify();
		}

		return StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body()))
		                   .expectFusion(Fuseable.SYNC);
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> operatorNextVerifierFusedConditionalSync(Scenario<I, O> scenario) {
		if ((scenario.fusionMode() & Fuseable.SYNC) != 0) {
			StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body())
			                                                   .filter(d -> true), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs =
							            ((Fuseable.QueueSubscription<O>) ((Receiver) s).upstream());

					            assertThat(qs.requestFusion(Fuseable.SYNC | THREAD_BARRIER)).isEqualTo(
							            fusionModeThreadBarrierSupport() & Fuseable.SYNC);

					            qs.size();
					            qs.isEmpty();
					            qs.clear();
					            assertThat(qs.isEmpty()).isTrue();
				            }
			            })
			            .thenCancel()
			            .verify();

			StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body())
			                                                   .filter(d -> true), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs =
							            ((Fuseable.QueueSubscription<O>) ((Receiver) s).upstream());
					            assertThat(qs.requestFusion(NONE)).isEqualTo(NONE);
				            }
			            })
			            .thenCancel()
			            .verify();
		}
		return StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body())
		                                                          .filter(d -> true))
		                   .expectFusion(Fuseable.SYNC);
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedAsync(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(up.as(scenario.body()))
		                   .expectFusion(Fuseable.ASYNC)
		                   .then(() -> testUnicastSource(scenario, up));
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedConditionalAsync(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(up.as(scenario.body())
		                             .filter(d -> true))
		                   .expectFusion(Fuseable.ASYNC)
		                   .then(() -> testUnicastSource(scenario, up));
	}

	@SuppressWarnings("unchecked")
	final void operatorNextVerifierFusedAsyncState(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		testUnicastSource(scenario, up);
		StepVerifier.create(up.as(scenario.body()), 0)
		            .consumeSubscriptionWith(s -> {
			            if (s instanceof Fuseable.QueueSubscription) {
				            Fuseable.QueueSubscription<O> qs =
						            ((Fuseable.QueueSubscription<O>) s);
				            qs.requestFusion(ASYNC);
				            if (up.actual != qs || scenario.prefetch() == UNSPECIFIED) {
				            	qs.size(); //touch undeterministic
				            }
				            else {
					            assertThat(qs.size()).isEqualTo(up.size());
				            }
				            try {
				            	qs.poll();
				            	qs.poll();
				            	qs.poll();
				            }
				            catch (Exception e) {
				            }
				            if (qs instanceof Trackable && ((Trackable) qs).getError() != null) {
					            assertThat(((Trackable) qs).getError()).hasMessage(
							            exception().getMessage());
					            if (up.actual != qs || scenario.prefetch() == UNSPECIFIED) {
						            qs.size(); //touch undeterministic
					            }
					            else {
						            assertThat(qs.size()).isEqualTo(up.size());
					            }
				            }
				            qs.clear();
				            assertThat(qs.size()).isEqualTo(0);
			            }
		            })
		            .thenCancel()
		            .verify();

		UnicastProcessor<I> up2 = UnicastProcessor.create();
		StepVerifier.create(up2.as(scenario.body()), 0)
		            .consumeSubscriptionWith(s -> {
			            if (s instanceof Fuseable.QueueSubscription) {
				            Fuseable.QueueSubscription<O> qs =
						            ((Fuseable.QueueSubscription<O>) s);
				            assertThat(qs.requestFusion(ASYNC | THREAD_BARRIER)).isEqualTo(
						            fusionModeThreadBarrierSupport() & ASYNC);
			            }
		            })
		            .thenCancel()
		            .verify();
	}

	@SuppressWarnings("unchecked")
	final void operatorNextVerifierFusedConditionalAsyncState(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		testUnicastSource(scenario, up);
		StepVerifier.create(up.as(scenario.body())
		                      .filter(d -> true), 0)
		            .consumeSubscriptionWith(s -> {
			            if (s instanceof Fuseable.QueueSubscription) {
				            Fuseable.QueueSubscription<O> qs =
						            ((Fuseable.QueueSubscription<O>) ((Receiver) s).upstream());
				            qs.requestFusion(ASYNC);
				            if (up.actual != qs || scenario.prefetch() == UNSPECIFIED) {
					            qs.size(); //touch undeterministic
				            }
				            else {
					            assertThat(qs.size()).isEqualTo(up.size());
				            }
				            try {
					            qs.poll();
					            qs.poll();
					            qs.poll();
				            }
				            catch (Exception e) {
				            }
				            if (qs instanceof Trackable && ((Trackable) qs).getError() != null) {
					            assertThat(((Trackable) qs).getError()).hasMessage(
							            exception().getMessage());
					            if (up.actual != qs || scenario.prefetch() == UNSPECIFIED) {
						            qs.size(); //touch undeterministic
					            }
					            else {
						            assertThat(qs.size()).isEqualTo(up.size());
					            }
				            }
				            qs.clear();
				            assertThat(qs.size()).isEqualTo(0);
			            }
		            })
		            .thenCancel()
		            .verify();

		UnicastProcessor<I> up2 = UnicastProcessor.create();
		StepVerifier.create(up2.as(scenario.body())
		                      .filter(d -> true), 0)
		            .consumeSubscriptionWith(s -> {
			            if (s instanceof Fuseable.QueueSubscription) {
				            Fuseable.QueueSubscription<O> qs =
						            ((Fuseable.QueueSubscription<O>) ((Receiver) s).upstream());
				            assertThat(qs.requestFusion(ASYNC | THREAD_BARRIER)).isEqualTo(
						            fusionModeThreadBarrierSupport() & ASYNC);
			            }
		            })
		            .thenCancel()
		            .verify();
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
		return StepVerifier.create(
				up.as(f -> new FluxFuseableExceptionOnPoll<>(f, exception()))
				  .as(scenario.body()))
		                   .then(() -> {
			if(up.actual != null) {
				up.actual.onError(exception());

				//verify drop path

				if (scenario.shouldHitDropErrorHookAfterTerminate()) {
					up.actual.onComplete();
					up.actual.onError(droppedException());
					assertThat(errorDropped.get()).isTrue();
				}
				if (scenario.shouldHitDropNextHookAfterTerminate()) {

					FluxFuseableExceptionOnPoll.next(up.actual, droppedItem());
					assertThat(nextDropped.get()).isTrue();
					if (FluxFuseableExceptionOnPoll.shouldTryNext(up.actual)) {
						nextDropped.set(false);
						FluxFuseableExceptionOnPoll.tryNext(up.actual, droppedItem());
						assertThat(nextDropped.get()).isTrue();
					}
				}

			}
		                   });
	}

	final StepVerifier.Step<O> operatorNextVerifierConditionalTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).hide()
		                                                          .as(scenario.body())
		                                                          .filter(filter -> true),
				3).consumeSubscriptionWith(s -> s.request(0));
	}

	final StepVerifier.Step<O> operatorNextVerifierBothConditional(Scenario<I, O> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario.body())
		                             .filter(filter -> true))
		                   .then(() -> testPublisherSource(scenario, ts));
	}

	final StepVerifier.Step<O> operatorNextVerifierBothConditionalCancel(Scenario<I, O> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario.body())
		                             .filter(filter -> true));
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
			                   if (scenario.shouldHitDropErrorHookAfterTerminate()) {
				                   ts.complete();
				                   ts.error(droppedException());
				                   assertThat(errorDropped.get()).isTrue();
			                   }
			                   if (scenario.shouldHitDropNextHookAfterTerminate()) {
				                   ts.next(droppedItem());
				                   assertThat(nextDropped.get()).isTrue();
			                   }
		                   });
	}

	protected Scenario<I, O> defaultScenarioOptions(Scenario<I, O> defaultOptions) {
		return defaultOptions;
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
			                   if (scenario.shouldHitDropNextHookAfterTerminate()) {
				                   ts.next(droppedItem());
				                   assertThat(nextDropped.get()).isTrue();
			                   }
		                   });
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedBothConditionalTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body())
		                                                          .filter(filter -> true),
				3).consumeSubscriptionWith(s -> s.request(0));
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierFusedSync(Scenario<I, O> scenario) {
		return StepVerifier.create(Flux.just(item(0), item(1))
		                               .as(f -> new FluxFuseableExceptionOnPoll<>(f, exception()))
		                               .as(scenario.body()))
		                   .expectFusion(scenario.fusionMode() & SYNC);
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierFusedAsync(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		up.onNext(item(0));
		return StepVerifier.create(up.as(f -> new FluxFuseableExceptionOnPoll<>(f, exception()))
		                             .as(scenario.body()))
		                   .expectFusion(scenario.fusionMode() & ASYNC);
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
		return StepVerifier.create(up
				.as(f -> new FluxFuseableExceptionOnPoll<>(f, exception()))
				.as(scenario.body())
				.filter(filter -> true))
		                   .then(() -> {
			                   if(up.actual != null) {
				                   up.actual.onError(exception());

				                   //verify drop path
				                   if (scenario.shouldHitDropErrorHookAfterTerminate()) {
					                   up.actual.onComplete();
					                   up.actual.onError(droppedException());
					                   assertThat(errorDropped.get()).isTrue();
				                   }
				                   if (scenario.shouldHitDropNextHookAfterTerminate()) {
					                   FluxFuseableExceptionOnPoll.next(up.actual, droppedItem());
					                   assertThat(nextDropped.get()).isTrue();

					                   if (FluxFuseableExceptionOnPoll.shouldTryNext(up.actual)) {
						                   nextDropped.set(false);
						                   FluxFuseableExceptionOnPoll.tryNext(up.actual, droppedItem());
						                   assertThat(nextDropped.get()).isTrue();
					                   }
				                   }

			                   }
		                   });
	}

	final StepVerifier.Step<O> operatorNextVerifier(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).hide()
		                                                          .as(scenario.body()));
	}

	final StepVerifier.Step<O> operatorNextVerifierFusedTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(finiteSourceOrDefault(scenario).as(scenario.body()),
				3).consumeSubscriptionWith(s -> s.request(0));
	}

	@SuppressWarnings("unchecked")
	final void touchTreeState(Object parent){
		if (parent == null) {
			return;
		}

		if (parent instanceof Loopback) {
			assertThat(((Loopback) parent).connectedInput()).isNotNull();
			((Loopback) parent).connectedOutput();
		}

		if (parent instanceof Producer) {
			assertThat(((Producer) parent).downstream()).isNotNull();
		}

		if (parent instanceof MultiProducer){
			MultiProducer p = ((MultiProducer)parent);
			if(p.downstreamCount() != 0 || p.hasDownstreams()){
				Iterator<?> it = p.downstreams();
				while(it.hasNext()){
					touchInner(it.next());
				}
			}
		}

		if(parent instanceof MultiReceiver){
			MultiReceiver p = ((MultiReceiver)parent);
			if(p.upstreamCount() != 0){
				Iterator<?> it = p.upstreams();
				while(it.hasNext()){
					touchInner(it.next());
				}
			}
		}
	}

	final void touchInner(Object t){
		if(t instanceof Trackable){
			Trackable o = (Trackable)t;
			o.requestedFromDownstream();
			o.expectedFromUpstream();
			o.getPending();
			o.getCapacity();
			o.getError();
			o.limit();
			o.isTerminated();
			o.isStarted();
			o.isCancelled();
		}
		if(t instanceof Producer){
			((Producer)t).downstream();
		}
		if(t instanceof Loopback){
			((Loopback)t).connectedInput();
			((Loopback)t).connectedOutput();
		}
		if(t instanceof Receiver){
			((Receiver)t).upstream();
		}
	}

	@After
	public void resetHooks() {
		Hooks.resetOnErrorDropped();
		Hooks.resetOnNextDropped();
		Hooks.resetOnOperator();
		Hooks.resetOnOperatorError();
	}

	@Before
	public final void initDefaultScenario() {
		defaultScenario = defaultScenarioOptions(new Scenario<>(null, null));
	}

	static final class FluxFuseableExceptionOnPoll<T> extends FluxSource<T, T> implements Fuseable{

		@SuppressWarnings("unchecked")
		static <I> void next(Subscriber<I> s, I item){
			((FuseableExceptionOnPollSubscriber<I>)s).actual.onNext(item);
		}
		@SuppressWarnings("unchecked")
		static <I> void tryNext(Subscriber<I> s, I item){
			((Fuseable.ConditionalSubscriber<I>)((FuseableExceptionOnPollSubscriber<I>) s).actual).tryOnNext(item);
		}

		@SuppressWarnings("unchecked")
		static <I> boolean shouldTryNext(Subscriber<I> s){
			return s instanceof FuseableExceptionOnPollSubscriber
					&& ((FuseableExceptionOnPollSubscriber)s).actual
					instanceof Fuseable.ConditionalSubscriber;
		}

		final RuntimeException exception;

		FluxFuseableExceptionOnPoll(Publisher<? extends T> source, RuntimeException exception) {
			super(source);
			this.exception = exception;
		}

		@Override
		public void subscribe(Subscriber<? super T> s) {
			source.subscribe(new FuseableExceptionOnPollSubscriber<>(s, exception));
		}

		static class FuseableExceptionOnPollSubscriber<T> implements
		                                                        QueueSubscription<T>,
		                                                        ConditionalSubscriber<T>{

			final Subscriber<? super T> actual;

			final RuntimeException exception;
			QueueSubscription<T> qs;
			FuseableExceptionOnPollSubscriber(Subscriber<? super T> actual,
					RuntimeException exception) {
				this.actual = actual;
				this.exception = exception;
			}

			@Override
			public boolean tryOnNext(T t) {
				throw exception;
			}

			@Override
			public void onSubscribe(Subscription s) {
				this.qs = Operators.as(s);
				actual.onSubscribe(this);
			}

			@Override
			public void onNext(T t) {
				if(t != null) {
					throw exception;
				}
				actual.onNext(null);
			}

			@Override
			public void onError(Throwable t) {
				actual.onError(t);
			}

			@Override
			public void onComplete() {
				actual.onComplete();
			}

			@Override
			public int requestFusion(int requestedMode) {
				return qs.requestFusion(requestedMode);
			}

			@Override
			public T poll() {
				T t = qs.poll();
				if(t != null) {
					throw exception;
				}
				return null;
			}

			@Override
			public int size() {
				return qs.size();
			}

			@Override
			public boolean isEmpty() {
				return qs.isEmpty();
			}

			@Override
			public void clear() {
				qs.clear();
			}

			@Override
			public void request(long n) {
				qs.request(n);
			}

			@Override
			public void cancel() {
				qs.cancel();
			}

		}
	}
}
