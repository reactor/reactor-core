/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.ParallelFlux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Stephane Maldini
 */
public class HooksTraceTest {

	@Test
	public void testTrace() {
		Hooks.onOperatorDebug();

		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
				Mono.fromCallable(() -> {
					throw new RuntimeException("test");
				})
				    .map(d -> d)
				    .block()
		).satisfies(r -> assertThat(r.getSuppressed()[0]).hasMessageContaining("Assembly trace from producer [reactor.core.publisher.MonoCallable]"));
	}

	@Test
	public void testTrace2() {
		Hooks.onOperatorDebug();

		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
			Mono.just(1)
			    .map(d -> {
				    throw new RuntimeException();
			    })
			    .filter(d -> true)
			    .doOnNext(d -> System.currentTimeMillis())
			    .map(d -> d)
			    .block()
		).satisfies(e -> assertThat(e.getSuppressed()[0])
				.hasMessageContaining("HooksTraceTest.java:")
				.hasMessageContaining("|_      Mono.map ⇢ at reactor.HooksTraceTest.lambda$testTrace2$")
		);
	}

	@Test
	public void testTrace3() {
		Hooks.onOperatorDebug();
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
				Flux.just(1)
				    .map(d -> {
					    throw new RuntimeException();
				    })
				    .share()
				    .filter(d -> true)
				    .doOnNext(d -> System.currentTimeMillis())
				    .map(d -> d)
				    .blockLast()
		).satisfies(e -> assertThat(e.getSuppressed()[0])
				.hasMessageContaining("HooksTraceTest.java:")
				.hasMessageContaining("|_    Flux.share ⇢ at reactor.HooksTraceTest.lambda$testTrace3$")
		);
	}

	@Test
	public void testTraceDefer() {
		Hooks.onOperatorDebug();
		try {
			//avoid wrapping this in yet another lambda (eg. AssertJ assertThatExceptionOfType().isThrownBy)
			//because in Java 8 at least it seems to cause the StackTraceElement to miss the method name: `lambda$null$xxx`
			Mono.defer(() -> Mono.just(1)
			                     .flatMap(d -> Mono.error(new IllegalStateException("boom")))
			                     .filter(d -> true)
			                     .doOnNext(d -> System.currentTimeMillis())
			                     .map(d -> d))
			    .block();
			fail("Expected IllegalStateException here");
		}
		catch (IllegalStateException ise) {
			assertThat(ise.getSuppressed()[0])
					.hasMessageContaining("HooksTraceTest.java:")
					.hasMessageContaining("*___Mono.flatMap ⇢ at reactor.HooksTraceTest.lambda$testTraceDefer$");
		}
	}

	@Test
	public void testTraceComposed() {
		Hooks.onOperatorDebug();
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
				Mono.just(1)
				    .flatMap(d -> Mono.error(new RuntimeException()))
				    .filter(d -> true)
				    .doOnNext(d -> System.currentTimeMillis())
				    .map(d -> d)
				    .block()
		).satisfies(e -> assertThat(e.getSuppressed()[0])
				.hasMessageContaining("HooksTraceTest.java:")
				.hasMessageContaining("*___Mono.flatMap ⇢ at reactor.HooksTraceTest.lambda$testTraceComposed$")
		);
	}

	@Test
	public void testTraceComposed2() {
		Hooks.onOperatorDebug();
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
				Flux.just(1)
				    .flatMap(d -> {
					    throw new RuntimeException();
				    })
				    .filter(d -> true)
				    .doOnNext(d -> System.currentTimeMillis())
				    .map(d -> d)
				    .blockLast()
		).satisfies(e -> assertThat(e.getSuppressed()[0])
				.hasMessageContaining("HooksTraceTest.java:")
				.hasMessageContaining("*___Flux.flatMap ⇢ at reactor.HooksTraceTest.lambda$testTraceComposed2$")
		);
	}

	@Test
	public void testOnLastPublisher() {
		List<Publisher> l = new ArrayList<>();
		Hooks.onLastOperator(p -> {
			l.add(p);
			return p;
		});
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .map(m -> m)
		                        .takeUntilOther(Mono.never())
		                        .flatMap(d -> Mono.just(d).hide()))
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(l).hasSize(5);
	}

	@Test
	public void testMultiReceiver() {
		Hooks.onOperatorDebug();
		//this will avoid relying on exceptions surfacing from assertion in error handler
		AssertionError[] assertionErrors = new AssertionError[3];
		List<Throwable> assemblyExceptions = new ArrayList<>();

		ConnectableFlux<?> t = Flux.empty()
			.then(Mono.defer(() -> {
				throw new RuntimeException();
			})).flux().publish();

		t.map(d -> d).subscribe(null, e -> {
			assemblyExceptions.add(e.getSuppressed()[0]);
			try {
				//live evaluation, error hasn't been emitted everywhere so we only have one occurrence so far
				assertThat(e.getSuppressed()[0]).hasMessageContaining("|_ Flux.publish ⇢ at reactor.HooksTraceTest.testMultiReceiver(HooksTraceTest.java:179)");
			}
			catch (AssertionError ae) {
				assertionErrors[0] = ae;
			}
		});

		t.filter(d -> true).subscribe(null, e -> {
			assemblyExceptions.add(e.getSuppressed()[0]);
			try {
				//live evaluation, error hasn't been emitted everywhere so we only have two occurrences so far
				assertThat(e.getSuppressed()[0]).hasMessageContaining("|_ Flux.publish ⇢ at reactor.HooksTraceTest.testMultiReceiver(HooksTraceTest.java:179) (observed 2 times)");
			}
			catch (AssertionError ae) {
				assertionErrors[1] = ae;
			}
		});
		t.distinct().subscribe(null, e -> {
			assemblyExceptions.add(e.getSuppressed()[0]);
			try {
				//live evaluation, error has been emitted everywhere so we have the three occurrences now
				// (note that the indentation has grown by 1 due to distinct operator getting in the mix)
				assertThat(e.getSuppressed()[0]).hasMessageContaining("|_  Flux.publish ⇢ at reactor.HooksTraceTest.testMultiReceiver(HooksTraceTest.java:179) (observed 3 times)");
			}
			catch (AssertionError ae) {
				assertionErrors[2] = ae;
			}
		});

		t.connect();

		assertThat(assertionErrors).allMatch(Objects::isNull);
		assertThat(assemblyExceptions)
			.hasSize(3)
			.allMatch(Objects::nonNull)
			.containsOnly(assemblyExceptions.get(0))
			.first(InstanceOfAssertFactories.THROWABLE)
			.hasMessageContaining("|_  Flux.publish ⇢ at reactor.HooksTraceTest.testMultiReceiver(HooksTraceTest.java:179) (observed 3 times)")
			.hasMessageNotContainingAny("(observed 2 times)");
	}

	@Test
	public void lastOperatorTest() {
		Hooks.onLastOperator(Operators.lift((sc, sub) ->
				new CoreSubscriber<Object>(){
					@Override
					public void onSubscribe(Subscription s) {
						sub.onSubscribe(s);
					}

					@Override
					public void onNext(Object o) {
						sub.onNext(((Integer)o) + 1);
					}

					@Override
					public void onError(Throwable t) {
						sub.onError(t);
					}

					@Override
					public void onComplete() {
						sub.onComplete();
					}
				}));

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .log(null, Level.OFF)
		                        .log(null, Level.OFF))
		            .expectNext(2, 3, 4)
		            .verifyComplete();

		StepVerifier.create(Mono.just(1)
		                        .log(null, Level.OFF)
		                        .log(null, Level.OFF))
		            .expectNext(2)
		            .verifyComplete();

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                        .log(null, Level.OFF)
		                        .log(null, Level.OFF))
		            .expectNext(2, 2)
		            .verifyComplete();

		Hooks.resetOnLastOperator();
	}

	@Test
	public void lastOperatorFilterTest() {
		Hooks.onLastOperator(Operators.lift(sc -> sc.tags()
		                                            .anyMatch(t -> t.getT1()
		                                                            .contains("metric")),
				(sc, sub) -> new CoreSubscriber<Object>() {
					@Override
					public void onSubscribe(Subscription s) {
						sub.onSubscribe(s);
					}

					@Override
					public void onNext(Object o) {
						sub.onNext(((Integer) o) + 1);
					}

					@Override
					public void onError(Throwable t) {
						sub.onError(t);
					}

					@Override
					public void onComplete() {
						sub.onComplete();
					}
				}));

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .tag("metric", "test")
		                        .log(null, Level.OFF)
		                        .log(null, Level.OFF))
		            .expectNext(2, 3, 4)
		            .verifyComplete();

		StepVerifier.create(Mono.just(1)
		                        .tag("metric", "test")
		                        .log(null, Level.OFF)
		                        .log(null, Level.OFF))
		            .expectNext(2)
		            .verifyComplete();

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                                .tag("metric", "test")
		                                .log(null, Level.OFF)
		                                .log(null, Level.OFF))
		            .expectNext(2, 2)
		            .verifyComplete();

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .log(null, Level.OFF)
		                        .log(null, Level.OFF))
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		StepVerifier.create(Mono.just(1)
		                        .log(null, Level.OFF)
		                        .log(null, Level.OFF))
		            .expectNext(1)
		            .verifyComplete();

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                                .log(null, Level.OFF)
		                                .log(null, Level.OFF))
		            .expectNext(1, 1)
		            .verifyComplete();
	}

	@Test
	public void eachOperatorTest() {
		Hooks.onEachOperator(Operators.lift((sc, sub) ->
				new CoreSubscriber<Object>(){
					@Override
					public void onSubscribe(Subscription s) {
						sub.onSubscribe(s);
					}

					@Override
					public void onNext(Object o) {
						sub.onNext(((Integer)o) + 1);
					}

					@Override
					public void onError(Throwable t) {
						sub.onError(t);
					}

					@Override
					public void onComplete() {
						sub.onComplete();
					}
				}));

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .log(null, Level.OFF)
		                        .log(null, Level.OFF))
		            .expectNext(4, 5, 6)
		            .verifyComplete();

		StepVerifier.create(Mono.just(1)
		                        .log(null, Level.OFF)
		                        .log(null, Level.OFF))
		            .expectNext(4)
		            .verifyComplete();

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                                .log(null, Level.OFF)
		                                .log(null, Level.OFF))
		            .expectNext(7, 7) //from now counts as an additional one
		            .verifyComplete();
	}

}
