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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Level;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Stephane Maldini
 */
public class HooksTest {

	void simpleFlux(){
		Flux.just(1)
		    .map(d -> d + 1)
		    .doOnNext(d -> {throw new RuntimeException("test");})
		    .collectList()
		    .onErrorReturn(Collections.singletonList(2))
		    .block();
	}

	static final class TestException extends RuntimeException {

		public TestException(String message) {
			super(message);
		}
	}


	@Test
	public void staticActivationOfOperatorDebug() {
		String oldProp = System.setProperty("reactor.trace.operatorStacktrace", "true");
		//this will be reset by the ReactorTestExecutionListener
		Hooks.GLOBAL_TRACE = Hooks.initStaticGlobalTrace();

		try {
			assertThat(Hooks.GLOBAL_TRACE).isTrue();
			//would throw NPE due to https://github.com/reactor/reactor-core/issues/985
			Mono.just("hello").subscribe();
		}
		finally {
			if (oldProp != null) {
				System.setProperty("reactor.trace.operatorStacktrace", oldProp);
			}
		}
	}

	@Test
	public void getOnEachOperatorHooksIsUnmodifiable() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(Hooks.getOnEachOperatorHooks()::clear);
	}

	@Test
	public void getOnLastOperatorHooksIsUnmodifiable() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(Hooks.getOnLastOperatorHooks()::clear);
	}

	@Test
	public void getOnOperatorErrorHooksIsUnmodifiable() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(Hooks.getOnOperatorErrorHooks()::clear);
	}

	@Test
	public void onEachOperatorSameLambdaSameNameAppliedOnce() {
		AtomicInteger applied = new AtomicInteger();
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook = p -> {
			applied.incrementAndGet();
			return p;
		};

		Hooks.onEachOperator(hook);
		Hooks.onEachOperator(hook);
		Hooks.onEachOperatorHook.apply(s -> {});

		assertThat(applied).hasValue(1);
	}

	@Test
	public void onEachOperatorSameLambdaDifferentNamesAppliedTwice() {
		AtomicInteger applied = new AtomicInteger();
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook = p -> {
			applied.incrementAndGet();
			return p;
		};

		Hooks.onEachOperator(hook);
		Hooks.onEachOperator("other", hook);
		Hooks.onEachOperatorHook.apply(s -> {});

		assertThat(applied).hasValue(2);
	}

	@Test
	public void onEachOperatorOneHookNoComposite() {
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook = p -> p;
		Hooks.onEachOperator(hook);

		assertThat(Hooks.onEachOperatorHook).isSameAs(hook);
	}

	@Test
	public void onEachOperatorNamedReplacesKeepsOrder() {
		List<String> applied = new ArrayList<>(3);

		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook1 = p -> {
			applied.add("h1");
			return p;
		};
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook2 = p -> {
			applied.add("h2");
			return p;
		};
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook3 = p -> {
			applied.add("h3");
			return p;
		};

		Hooks.onEachOperator("1", hook1);
		Hooks.onEachOperator("2", hook2);
		Hooks.onEachOperatorHook.apply(s -> {});

		assertThat(Hooks.getOnEachOperatorHooks())
				.containsOnlyKeys("1", "2");
		assertThat(Hooks.getOnEachOperatorHooks().values())
				.containsExactly(hook1, hook2);
		assertThat(applied).containsExactly("h1", "h2");

		applied.clear();
		Hooks.onEachOperator("1", hook3);
		Hooks.onEachOperatorHook.apply(s -> {});

		assertThat(Hooks.getOnEachOperatorHooks())
				.containsOnlyKeys("1", "2");
		assertThat(Hooks.getOnEachOperatorHooks().values())
				.containsExactly(hook3, hook2);
		assertThat(applied).containsExactly("h3", "h2");
	}

	@Test
	public void onEachOperatorResetSpecific() {
		List<String> applied = new ArrayList<>(3);
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook1 = p -> {
			applied.add("h1");
			return p;
		};
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook2 = p -> {
			applied.add("h2");
			return p;
		};

		Hooks.onEachOperator("1", hook1);
		Hooks.onEachOperator(hook2);
		Hooks.onEachOperatorHook.apply(s -> {});

		assertThat(Hooks.getOnEachOperatorHooks()).hasSize(2);
		assertThat(applied).containsExactly("h1", "h2");

		applied.clear();
		Hooks.resetOnEachOperator("1");
		Hooks.onEachOperatorHook.apply(s -> {});

		assertThat(Hooks.getOnEachOperatorHooks()).hasSize(1);
		assertThat(applied).containsExactly("h2");
	}

	@Test
	public void onEachOperatorReset() {
		Hooks.onEachOperator("some", p -> p);

		assertThat(Hooks.onEachOperatorHook).isNotNull();
		assertThat(Hooks.getOnEachOperatorHooks()).hasSize(1);

		Hooks.resetOnEachOperator();

		assertThat(Hooks.onEachOperatorHook).isNull();
		assertThat(Hooks.getOnEachOperatorHooks()).isEmpty();
	}

	@Test
	public void onEachOperatorClearByName() {
		Hooks.onEachOperator("some", p -> p);
		Hooks.onEachOperator("other", p -> p);

		assertThat(Hooks.onEachOperatorHook).isNotNull();
		assertThat(Hooks.getOnEachOperatorHooks()).hasSize(2);

		Hooks.resetOnEachOperator("some");

		assertThat(Hooks.onEachOperatorHook).isNotNull();
		assertThat(Hooks.getOnEachOperatorHooks())
				.hasSize(1)
				.containsOnlyKeys("other");

		Hooks.resetOnEachOperator("other");

		assertThat(Hooks.onEachOperatorHook).isNull();
		assertThat(Hooks.getOnEachOperatorHooks()).isEmpty();
	}

	@Test
	public void onLastOperatorSameLambdaSameNameAppliedOnce() {
		AtomicInteger applied = new AtomicInteger();
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook = p -> {
			applied.incrementAndGet();
			return p;
		};

		Hooks.onLastOperator(hook);
		Hooks.onLastOperator(hook);
		Hooks.onLastOperatorHook.apply(s -> {});

		assertThat(applied).hasValue(1);
	}

	@Test
	public void onLastOperatorSameLambdaDifferentNamesAppliedTwice() {
		AtomicInteger applied = new AtomicInteger();
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook = p -> {
			applied.incrementAndGet();
			return p;
		};

		Hooks.onLastOperator(hook);
		Hooks.onLastOperator("other", hook);
		Hooks.onLastOperatorHook.apply(s -> {});

		assertThat(applied).hasValue(2);
	}

	@Test
	public void onLastOperatorOneHookNoComposite() {
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook = p -> p;
		Hooks.onLastOperator(hook);

		assertThat(Hooks.onLastOperatorHook).isSameAs(hook);
	}

	@Test
	public void onLastOperatorNamedReplacesKeepsOrder() {
		List<String> applied = new ArrayList<>(3);

		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook1 = p -> {
			applied.add("h1");
			return p;
		};
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook2 = p -> {
			applied.add("h2");
			return p;
		};
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook3 = p -> {
			applied.add("h3");
			return p;
		};

		Hooks.onLastOperator("1", hook1);
		Hooks.onLastOperator("2", hook2);
		Hooks.onLastOperatorHook.apply(s -> {});

		assertThat(Hooks.getOnLastOperatorHooks())
				.containsOnlyKeys("1", "2");
		assertThat(Hooks.getOnLastOperatorHooks().values())
				.containsExactly(hook1, hook2);
		assertThat(applied).containsExactly("h1", "h2");

		applied.clear();
		Hooks.onLastOperator("1", hook3);
		Hooks.onLastOperatorHook.apply(s -> {});

		assertThat(Hooks.getOnLastOperatorHooks())
				.containsOnlyKeys("1", "2");
		assertThat(Hooks.getOnLastOperatorHooks().values())
				.containsExactly(hook3, hook2);
		assertThat(applied).containsExactly("h3", "h2");
	}

	@Test
	public void onLastOperatorResetSpecific() {
		List<String> applied = new ArrayList<>(3);
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook1 = p -> {
			applied.add("h1");
			return p;
		};
		Function<? super Publisher<Object>, ? extends Publisher<Object>> hook2 = p -> {
			applied.add("h2");
			return p;
		};

		Hooks.onLastOperator("1", hook1);
		Hooks.onLastOperator(hook2);
		Hooks.onLastOperatorHook.apply(s -> {});

		assertThat(Hooks.getOnLastOperatorHooks()).hasSize(2);
		assertThat(applied).containsExactly("h1", "h2");

		applied.clear();
		Hooks.resetOnLastOperator("1");
		Hooks.onLastOperatorHook.apply(s -> {});

		assertThat(Hooks.getOnLastOperatorHooks()).hasSize(1);
		assertThat(applied).containsExactly("h2");
	}

	@Test
	public void onLastOperatorReset() {
		Hooks.onLastOperator("some", p -> p);

		assertThat(Hooks.onLastOperatorHook).isNotNull();
		assertThat(Hooks.getOnLastOperatorHooks()).hasSize(1);

		Hooks.resetOnLastOperator();

		assertThat(Hooks.onLastOperatorHook).isNull();
		assertThat(Hooks.getOnLastOperatorHooks()).isEmpty();
	}

	@Test
	public void onLastOperatorClearByName() {
		Hooks.onLastOperator("some", p -> p);
		Hooks.onLastOperator("other", p -> p);

		assertThat(Hooks.onLastOperatorHook).isNotNull();
		assertThat(Hooks.getOnLastOperatorHooks()).hasSize(2);

		Hooks.resetOnLastOperator("some");

		assertThat(Hooks.onLastOperatorHook).isNotNull();
		assertThat(Hooks.getOnLastOperatorHooks())
				.hasSize(1)
				.containsOnlyKeys("other");

		Hooks.resetOnLastOperator("other");

		assertThat(Hooks.onLastOperatorHook).isNull();
		assertThat(Hooks.getOnLastOperatorHooks()).isEmpty();
	}

	@Test
	public void onOperatorErrorSameLambdaSameNameAppliedOnce() {
		AtomicInteger applied = new AtomicInteger();
		BiFunction<Throwable, Object, Throwable> hook = (t, s) -> {
			applied.incrementAndGet();
			return t;
		};

		Hooks.onOperatorError(hook);
		Hooks.onOperatorError(hook);
		Hooks.onOperatorErrorHook.apply(new IllegalStateException("boom"), "foo");

		assertThat(applied).hasValue(1);
	}

	@Test
	public void onOperatorErrorSameLambdaDifferentNamesAppliedTwice() {
		AtomicInteger applied = new AtomicInteger();
		BiFunction<Throwable, Object, Throwable> hook = (t, s) -> {
			applied.incrementAndGet();
			return t;
		};

		Hooks.onOperatorError(hook);
		Hooks.onOperatorError("other", hook);
		Hooks.onOperatorErrorHook.apply(new IllegalStateException("boom"), "foo");

		assertThat(applied).hasValue(2);
	}

	@Test
	public void onOperatorErrorOneHookNoComposite() {
		BiFunction<Throwable, Object, Throwable> hook = (t, s) -> t;

		Hooks.onOperatorError(hook);

		assertThat(Hooks.onOperatorErrorHook).isSameAs(hook);
	}

	@Test
	public void onOperatorErrorNamedReplacesKeepsOrder() {
		List<String> applied = new ArrayList<>(3);
		BiFunction<Throwable, Object, Throwable> hook1 = (t, s) -> {
			applied.add("h1");
			return t;
		};
		BiFunction<Throwable, Object, Throwable> hook2 = (t, s) -> {
			applied.add("h2");
			return t;
		};
		BiFunction<Throwable, Object, Throwable> hook3 = (t, s) -> {
			applied.add("h3");
			return t;
		};

		Hooks.onOperatorError("1", hook1);
		Hooks.onOperatorError("2", hook2);
		Hooks.onOperatorErrorHook.apply(new IllegalStateException("boom"), "foo");

		assertThat(Hooks.getOnOperatorErrorHooks())
				.containsOnlyKeys("1", "2");
		assertThat(Hooks.getOnOperatorErrorHooks().values())
				.containsExactly(hook1, hook2);
		assertThat(applied).containsExactly("h1", "h2");

		applied.clear();
		Hooks.onOperatorError("1", hook3);
		Hooks.onOperatorErrorHook.apply(new IllegalStateException("boom2"), "bar");

		assertThat(Hooks.getOnOperatorErrorHooks())
				.containsOnlyKeys("1", "2");
		assertThat(Hooks.getOnOperatorErrorHooks().values())
				.containsExactly(hook3, hook2);
		assertThat(applied).containsExactly("h3", "h2");
	}

	@Test
	public void onOperatorErrorResetSpecific() {
		List<String> applied = new ArrayList<>(3);
		BiFunction<Throwable, Object, Throwable> hook1 = (t, s) -> {
			applied.add("h1");
			return t;
		};
		BiFunction<Throwable, Object, Throwable> hook2 = (t, s) -> {
			applied.add("h2");
			return t;
		};

		Hooks.onOperatorError("1", hook1);
		Hooks.onOperatorError(hook2);
		Hooks.onOperatorErrorHook.apply(new IllegalStateException("boom"), "foo");

		assertThat(Hooks.getOnOperatorErrorHooks()).hasSize(2);
		assertThat(applied).containsExactly("h1", "h2");

		applied.clear();
		Hooks.resetOnOperatorError("1");
		Hooks.onOperatorErrorHook.apply(new IllegalStateException("boom2"), "bar");

		assertThat(Hooks.getOnOperatorErrorHooks()).hasSize(1);
		assertThat(applied).containsExactly("h2");
	}

	@Test
	public void onOperatorErrorReset() {
		Hooks.onOperatorError("some", (t, v) -> t);

		assertThat(Hooks.onOperatorErrorHook).isNotNull();
		assertThat(Hooks.getOnOperatorErrorHooks()).hasSize(1);

		Hooks.resetOnOperatorError();

		assertThat(Hooks.onOperatorErrorHook).isNull();
		assertThat(Hooks.getOnOperatorErrorHooks()).isEmpty();
	}

	@Test
	public void onOperatorErrorClearByName() {
		Hooks.onOperatorError("some", (t, v) -> t);
		Hooks.onOperatorError("other", (t, v) -> t);

		assertThat(Hooks.onOperatorErrorHook).isNotNull();
		assertThat(Hooks.getOnOperatorErrorHooks()).hasSize(2);

		Hooks.resetOnOperatorError("some");

		assertThat(Hooks.onOperatorErrorHook).isNotNull();
		assertThat(Hooks.getOnOperatorErrorHooks())
				.hasSize(1)
				.containsOnlyKeys("other");

		Hooks.resetOnOperatorError("other");

		assertThat(Hooks.onOperatorErrorHook).isNull();
		assertThat(Hooks.getOnOperatorErrorHooks()).isEmpty();
	}

	@Test
	public void errorHooks() throws Exception {
		Hooks.onOperatorError((e, s) -> new TestException(s.toString()));
		Hooks.onNextDropped(d -> {
			throw new TestException(d.toString());
		});
		Hooks.onErrorDropped(e -> {
			throw new TestException("errorDrop");
		});

		Throwable w = Operators.onOperatorError(null, new Exception(), "hello", Context.empty());

		assertThat(w).isInstanceOf(TestException.class);
		assertThat(w).hasMessage("hello");

		assertThatExceptionOfType(TestException.class).isThrownBy(() -> {
			Operators.onNextDropped("hello", Context.empty());
		}).withMessage("hello");

		assertThatExceptionOfType(TestException.class).isThrownBy(() -> {
			Operators.onErrorDropped(new Exception(), Context.empty());
		}).withMessage("errorDrop");
	}

	@Test
	public void accumulatingHooks() throws Exception {
		AtomicReference<String> ref = new AtomicReference<>();
		Hooks.onNextDropped(d -> {
			ref.set(d.toString());
		});
		Hooks.onNextDropped(d -> {
			ref.set(ref.get()+"bar");
		});

		Operators.onNextDropped("foo", Context.empty());

		assertThat(ref).hasValue("foobar");

		Hooks.onErrorDropped(d -> {
			ref.set(d.getMessage());
		});
		Hooks.onErrorDropped(d -> {
			ref.set(ref.get()+"bar");
		});

		Operators.onErrorDropped(new Exception("foo"), Context.empty());

		assertThat(ref).hasValue("foobar");

		Hooks.resetOnErrorDropped();


		Hooks.onOperatorError((error, d) -> {
			ref.set(d.toString());
			return new Exception("bar");
		});
		Hooks.onOperatorError((error, d) -> {
			ref.set(ref.get()+error.getMessage());
			return error;
		});

		Operators.onOperatorError(null, null, "foo", Context.empty());

		assertThat(ref).hasValue("foobar");

		Hooks.resetOnOperatorError();


		AtomicReference<Publisher> hook = new AtomicReference<>();
		AtomicReference<Object> hook2 = new AtomicReference<>();
		Hooks.onEachOperator(h -> {
			Flux<Object> publisher = Hooks.convertToFluxBypassingHooks(
					TestPublisher.create());
			hook.set(publisher);
			return publisher;
		});
		Hooks.onEachOperator(h -> {
			hook2.set(h);
			return h;
		});

		Flux.just("test").filter(d -> true).subscribe();

		assertThat(hook.get()).isNotNull().isEqualTo(hook2.get());
		Hooks.resetOnEachOperator();

		hook.set(null);
		hook2.set(null);

		Hooks.onLastOperator(h -> {
			final Flux<Object> publisher = Hooks.convertToFluxBypassingHooks(
					TestPublisher.create());
			hook.set(publisher);
			return publisher;
		});
		Hooks.onLastOperator(h -> {
			hook2.set(h);
			return h;
		});

		Flux.just("test").filter(d -> true).subscribe();

		assertThat(hook.get()).isNotNull().isEqualTo(hook2.get());
	}


	@Test
	public void parallelModeFused() {
		Hooks.onOperatorDebug();

		Hooks.onEachOperator(p -> {
			System.out.println(Scannable.from(p).stepName());
			return p;
		});

		Flux<Integer> source = Mono.just(1)
		                           .flux()
		                           .repeat(999)
		                           .publish()
		                           .autoConnect();
		int ncpu = Math.max(8,
				Runtime.getRuntime()
				       .availableProcessors());

			Scheduler scheduler = Schedulers.newParallel("test", ncpu);

			try {
				Flux<Integer> result = ParallelFlux.from(source, ncpu)
				                                   .runOn(scheduler)
				                                   .map(v -> v + 1)
				                                   .log("test", Level.INFO, true, SignalType.ON_SUBSCRIBE)
				                                   .sequential();

				AssertSubscriber<Integer> ts = AssertSubscriber.create();

				result.subscribe(ts);

				ts.await(Duration.ofSeconds(10));

				ts.assertSubscribed()
				  .assertValueCount(1000)
				  .assertComplete()
				  .assertNoError();
			}
			finally {
				scheduler.dispose();
			}

	}

	@Test
	public void verboseExtension() {
		Queue<String> q = new LinkedTransferQueue<>();
		Hooks.onEachOperator(p -> {
			q.offer(p.toString());
			return p;
		});
		Hooks.onOperatorDebug();

		simpleFlux();

		assertThat(q.toArray()).containsExactly(
				"FluxJust",
				"FluxMapFuseable",
				"FluxPeekFuseable",
				"MonoCollectList",
				"MonoOnErrorResume",
				"MonoJust"
				);


		q.clear();
		Hooks.resetOnEachOperator();

		Hooks.onEachOperator(p -> {
			q.offer(p.toString());
			return p;
		});

		simpleFlux();

		assertThat(q.toArray()).containsExactly(
				"FluxJust",
				"FluxMapFuseable",
				"FluxPeekFuseable",
				"MonoCollectList",
				"MonoOnErrorResume",
				"MonoJust");

		q.clear();
		Hooks.resetOnEachOperator();

		simpleFlux();

		assertThat(q.toArray()).isEmpty();
	}

	@Test
	public void testOnLastPublisher() throws Exception {
		List<Publisher> l = new ArrayList<>();
		Hooks.onLastOperator(p -> {
			System.out.println(Scannable.from(p).parents().count());
			System.out.println(Scannable.from(p).stepName());
			l.add(p);
			return p;
		});
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .map(m -> m)
		                        .takeUntilOther(Mono.never())
		                        .flatMap(d -> Mono.just(d).hide()))
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		Hooks.resetOnLastOperator();

		assertThat(l).hasSize(5);
	}

	@Test
	public void testMultiReceiver() throws Exception {
		Hooks.onOperatorDebug();
		ConnectableFlux<?> t = Flux.empty()
		    .then(Mono.defer(() -> {
			    throw new RuntimeException();
		    })).flux().publish();

		t.map(d -> d).subscribe(null,
				e -> assertThat(e.getSuppressed()[0]).hasMessageContaining("\t|_ Flux.publish"));

		t.filter(d -> true).subscribe(null, e -> assertThat(e.getSuppressed()[0]).hasMessageContaining("|_____ Flux.publish"));
		t.distinct().subscribe(null, e -> assertThat(e.getSuppressed()[0]).hasMessageContaining("_________  Flux.publish"));

		t.connect();
	}

	@Test
	public void onNextDroppedFailReplaces() {
		AtomicReference<Object> dropHook = new AtomicReference<>();
		Publisher<Integer> p = s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext(1);
			s.onNext(2);
			s.onNext(3);
		};
		List<Integer> seen = new ArrayList<>();

		try {
			Hooks.onNextDropped(dropHook::set);
			Hooks.onNextDroppedFail();

			assertThatExceptionOfType(RuntimeException.class)
					.isThrownBy(() -> Flux.from(p).take(2).subscribe(seen::add))
					.isInstanceOf(RuntimeException.class)
					.matches(Exceptions::isCancel);

			assertThat(seen).containsExactly(1, 2);
			assertThat(dropHook.get()).isNull();
		}
		finally {
			Hooks.resetOnNextDropped();
		}
	}

	@Test
	public void lastOperatorLiftsFlux() {
		AtomicInteger liftCounter = new AtomicInteger();
		Hooks.onLastOperator(Operators.lift((sc, sub) -> liftSubscriber(sc, sub, liftCounter)));

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .log()
		                        .log())
		            .expectNext(101, 102, 103)
		            .verifyComplete();

		assertThat(liftCounter).hasValue(1);
	}

	@Test
	public void lastOperatorLiftsMono() {
		AtomicInteger liftCounter = new AtomicInteger();
		Hooks.onLastOperator(Operators.lift((sc, sub) -> liftSubscriber(sc, sub, liftCounter)));

		StepVerifier.create(Mono.just(1)
		                        .log()
		                        .log())
		            .expectNext(101)
		            .verifyComplete();
	}

	@Test
	public void lastOperatorLiftsParallelFlux() {
		AtomicInteger liftCounter = new AtomicInteger();
		Hooks.onLastOperator(Operators.lift((sc, sub) -> liftSubscriber(sc, sub, liftCounter)));

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                                .log()
		                                .log())
		            .expectNext(101, 101)
		            .verifyComplete();
	}

	@Test
	public void lastOperatorFilteredLiftFlux() {
		AtomicInteger liftCounter = new AtomicInteger();
		Hooks.onLastOperator(Operators.lift(sc -> sc.tags()
		                                            .anyMatch(t -> t.getT1().contains("metric")),
				(sc, sub) -> liftSubscriber(sc, sub, liftCounter)));

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .tag("metric", "test")
		                        .log()
		                        .log())
		            .expectNext(101, 102, 103)
		            .verifyComplete();

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .log()
		                        .log())
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	public void lastOperatorFilteredLiftMono() {
		AtomicInteger liftCounter = new AtomicInteger();
		Hooks.onLastOperator(Operators.lift(sc -> sc.tags()
		                                            .anyMatch(t -> t.getT1().contains("metric")),
				(sc, sub) -> liftSubscriber(sc, sub, liftCounter)));

		StepVerifier.create(Mono.just(1)
		                        .tag("metric", "test")
		                        .log()
		                        .log())
		            .expectNext(101)
		            .verifyComplete();

		StepVerifier.create(Mono.just(1)
		                        .log()
		                        .log())
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void lastOperatorFilteredLiftParallelFlux() {
		AtomicInteger liftCounter = new AtomicInteger();
		Hooks.onLastOperator(Operators.lift(sc -> sc.tags()
		                                            .anyMatch(t -> t.getT1().contains("metric")),
				(sc, sub) -> liftSubscriber(sc, sub, liftCounter)));

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                                .tag("metric", "test")
		                                .log()
		                                .log())
		            .expectNext(101, 101)
		            .verifyComplete();

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                                .log()
		                                .log())
		            .expectNext(1, 1)
		            .verifyComplete();
	}

	@Test
	public void eachOperatorLiftsFlux() {
		AtomicInteger liftCounter = new AtomicInteger();
		Hooks.onEachOperator(Operators.lift((sc, sub) -> liftSubscriber(sc, sub, liftCounter)));

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .log()
		                        .log())
		            .expectNext(301, 302, 303)
		            .verifyComplete();
	}

	@Test
	public void eachOperatorLiftsMono() {
		AtomicInteger liftCounter = new AtomicInteger();
		Hooks.onEachOperator(Operators.lift((sc, sub) -> liftSubscriber(sc, sub, liftCounter)));

		StepVerifier.create(Mono.just(1)
		                        .log()
		                        .log())
		            .expectNext(301)
		            .verifyComplete();
	}

	@Test
	public void eachOperatorLiftsParallelFlux() {
		AtomicInteger liftCounter = new AtomicInteger();
		Hooks.onEachOperator(Operators.lift((sc, sub) -> liftSubscriber(sc, sub, liftCounter)));

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1)) //internally converts each rail to a Flux using Flux.from
		                                .log()
		                                .log())
		            .expectNext(601, 601) //6x lifts => just,Flux.from,ParallelFlux.from,log,log, back to sequential (shared)
		            .verifyComplete();

		assertThat(liftCounter).hasValue(11); //11x total lifts: 2xjust, 2xfrom, 2xparallelFrom,4xlog,sequential
	}

	@Test
	public void fluxWrapFluxSourceDoesntCallAssemblyHook() {
		final Flux<Integer> source = Flux.range(1, 10);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToFluxBypassingHooks(source);
		Flux.wrap(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxWrapScalarJustDoesntCallAssemblyHook() {
		final Mono<String> source = Mono.just("scalarJust");

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToFluxBypassingHooks(source);
		Flux.wrap(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxWrapScalarErrorDoesntCallAssemblyHook() {
		final Mono<Object> source = Mono.error(new IllegalStateException("scalarError"));

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToFluxBypassingHooks(source);
		Flux.wrap(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxWrapScalarEmptyDoesntCallAssemblyHook() {
		final Mono<Object> source = Mono.empty();

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToFluxBypassingHooks(source);
		Flux.wrap(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxWrapMonoFuseableDoesntCallAssemblyHook() {
		Mono<String> source = Mono.just("monoFuseable").map(i -> i);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToFluxBypassingHooks(source);
		Flux.wrap(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxWrapMonoNormalDoesntCallAssemblyHook() {
		final Mono<String> source = Mono.just("monoNormal")
		                                .hide()
		                                .map(i -> i);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToFluxBypassingHooks(source);
		Flux.wrap(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxWrapPublisherDoesntCallAssemblyHook() {
		Publisher<String> publisher = sub -> sub.onSubscribe(Operators.emptySubscription());

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToFluxBypassingHooks(publisher);
		Flux.wrap(publisher);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void monoWrapMonoDoesntCallAssemblyHook() {
		final Mono<Integer> source = Mono.just(1);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToMonoBypassingHooks(source, true);
		Hooks.convertToMonoBypassingHooks(source, false);
		Mono.wrap(source, true);
		Mono.wrap(source, false);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void monoWrapFluxWrappingMonoDoesntCallAssemblyHook() {
		final Flux<Integer> source = Flux.from(Mono.just(1).hide());

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToMonoBypassingHooks(source, true);
		Hooks.convertToMonoBypassingHooks(source, false);
		Mono.wrap(source, true);
		Mono.wrap(source, false);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void monoWrapCallableFluxDoesntCallAssemblyHook() {
		final Flux<Integer> source = Flux.just(1);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToMonoBypassingHooks(source, true);
		Hooks.convertToMonoBypassingHooks(source, false);
		Mono.wrap(source, true);
		Mono.wrap(source, false);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void monoWrapFluxDoesntCallAssemblyHook() {
		final Flux<Integer> source = Flux.just(1).hide();

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToMonoBypassingHooks(source, true);
		Hooks.convertToMonoBypassingHooks(source, false);
		Mono.wrap(source, true);
		Mono.wrap(source, false);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void monoWrapPublisherDoesntCallAssemblyHook() {
		final Publisher<Integer> source = TestPublisher.create();
		Assertions.assertThat(source).isNotInstanceOf(Flux.class); //smoke test this is a Publisher

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Hooks.convertToMonoBypassingHooks(source, true);
		Hooks.convertToMonoBypassingHooks(source, false);
		Mono.wrap(source, true);
		Mono.wrap(source, false);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxWrapToMonoScalarEmptyDoesntCallAssemblyHook() {
		@SuppressWarnings("unchecked")
		Callable<Integer> source = (Callable<Integer>) Flux.empty();

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.wrapToMono(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxWrapToMonoScalarValuedDoesntCallAssemblyHook() {
		@SuppressWarnings("unchecked")
		Callable<Integer> source = (Callable<Integer>) Flux.just(1);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.wrapToMono(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxWrapToMonoScalarErrorDoesntCallAssemblyHook() {
		@SuppressWarnings("unchecked")
		Callable<Integer> source = (Callable<Integer>) Flux.error(new IllegalStateException("boom"));

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.wrapToMono(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxWrapToMonoVanillaCallableDoesntCallAssemblyHook() {
		Callable<Integer> source = () -> 1;

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.wrapToMono(source);
		Assertions.assertThat(wrappedCount).hasValue(0);
	}

	private static CoreSubscriber<Object> liftSubscriber(Scannable scannable, CoreSubscriber<? super Object> sub, AtomicInteger liftCounter) {
		liftCounter.incrementAndGet();
		return new CoreSubscriber<Object>() {

			@Override
			public void onSubscribe(Subscription s) {
				sub.onSubscribe(s);
			}

			@Override
			public void onNext(Object o) {
				System.out.println("Lifting " + o + " out of " + scannable.stepName());
				sub.onNext((Integer) o + 100);
			}

			@Override
			public void onError(Throwable t) {
				sub.onError(t);
			}

			@Override
			public void onComplete() {
				sub.onComplete();
			}
		};
	}
}
