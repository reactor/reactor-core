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

package reactor;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class HooksTraceTest {

	@Test
	public void testTrace() throws Exception {
		Hooks.onOperatorDebug();
		try {
			Mono.fromCallable(() -> {
				throw new RuntimeException();
			})
			    .map(d -> d)
			    .block();
		}
		catch(Exception e){
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains("MonoCallable"));
			return;
		}
		finally {
			Hooks.resetOnOperatorDebug();
		}
		throw new IllegalStateException();
	}

	@Test
	public void testTrace2() throws Exception {
		Hooks.onOperatorDebug();

		try {
			Mono.just(1)
			    .map(d -> {
				    throw new RuntimeException();
			    })
			    .filter(d -> true)
			    .doOnNext(d -> System.currentTimeMillis())
			    .map(d -> d)
			    .block();
		}
		catch(Exception e){
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
					("HooksTraceTest.java:"));
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains("|_      Mono.map ⇢ at reactor.HooksTraceTest.testTrace2(HooksTraceTest.java:"));
			return;
		}
		finally {
			Hooks.resetOnOperatorDebug();
		}
		throw new IllegalStateException();
	}

	@Test
	public void testTrace3() throws Exception {
		Hooks.onOperatorDebug();
		try {
			Flux.just(1)
			    .map(d -> {
				    throw new RuntimeException();
			    })
			    .share()
			    .filter(d -> true)
			    .doOnNext(d -> System.currentTimeMillis())
			    .map(d -> d)
			    .blockLast();
		}
		catch(Exception e){
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
					("HooksTraceTest.java:"));
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains("|_    Flux.share ⇢ at reactor.HooksTraceTest.testTrace3(HooksTraceTest.java:"));
			return;
		}
		finally {
			Hooks.resetOnOperatorDebug();
		}
		throw new IllegalStateException();
	}

	@Test
	public void testTraceDefer() throws Exception {
		Hooks.onOperatorDebug();
		try {
			Mono.defer(() -> Mono.just(1)
			                     .flatMap(d -> Mono.error(new RuntimeException()))
			                     .filter(d -> true)
			                     .doOnNext(d -> System.currentTimeMillis())
			                     .map(d -> d))
			    .block();
		}
		catch(Exception e){
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
					("HooksTraceTest.java:"));
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains("|_  Mono.flatMap ⇢ at reactor.HooksTraceTest.lambda$testTraceDefer$14(HooksTraceTest.java:"));
			return;
		}
		finally {
			Hooks.resetOnOperatorDebug();
		}
		throw new IllegalStateException();
	}

	@Test
	public void testTraceComposed() throws Exception {
		Hooks.onOperatorDebug();
		try {
			Mono.just(1)
			    .flatMap(d -> Mono.error(new RuntimeException()))
			    .filter(d -> true)
			    .doOnNext(d -> System.currentTimeMillis())
			    .map(d -> d)
			    .block();
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage()
			                                      .contains("HooksTraceTest.java:"));
			Assert.assertTrue(e.getSuppressed()[0].getMessage()
			                                      .contains("|_  Mono.flatMap ⇢ at reactor.HooksTraceTest.testTraceComposed(HooksTraceTest.java:"));
			return;
		}
		finally {
			Hooks.resetOnOperatorDebug();
		}
		throw new IllegalStateException();
	}

	@Test
	public void testTraceComposed2() throws Exception {
		Hooks.onOperatorDebug();
		try {
			Flux.just(1)
			    .flatMap(d -> {
				    throw new RuntimeException();
			    })
			    .filter(d -> true)
			    .doOnNext(d -> System.currentTimeMillis())
			    .map(d -> d)
			    .blockLast();
		}
		catch(Exception e){
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
					("HooksTraceTest.java:"));
			assertThat(e.getSuppressed()[0].getMessage()).contains("|_  Flux.flatMap ⇢ at reactor.HooksTraceTest.testTraceComposed2(HooksTraceTest.java:");
			return;
		}
		finally {
			Hooks.resetOnOperatorDebug();
		}
		throw new IllegalStateException();
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
		try {

			ConnectableFlux<?> t = Flux.empty()
			    .then(Mono.defer(() -> {
				    throw new RuntimeException();
			    })).flux().publish();

			t.map(d -> d).subscribe(null,
					e -> assertThat(e.getSuppressed()[0].getMessage()).contains("\t|_ Flux.publish"));

			t.filter(d -> true).subscribe(null, e -> {
				assertThat(e.getSuppressed()[0].getMessage()).contains("\t|_____ Flux.publish");
			});
			t.distinct().subscribe(null, e -> {
				assertThat(e.getSuppressed()[0].getMessage()).contains("\t|_________  Flux.publish");
			});

			t.connect();
		}
		finally {
			Hooks.resetOnOperatorDebug();
		}
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
		                        .log()
		                        .log())
		            .expectNext(2, 3, 4)
		            .verifyComplete();

		StepVerifier.create(Mono.just(1)
		                        .log()
		                        .log())
		            .expectNext(2)
		            .verifyComplete();

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                        .log()
		                        .log())
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
		                        .log()
		                        .log())
		            .expectNext(2, 3, 4)
		            .verifyComplete();

		StepVerifier.create(Mono.just(1)
		                        .tag("metric", "test")
		                        .log()
		                        .log())
		            .expectNext(2)
		            .verifyComplete();

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                                .tag("metric", "test")
		                                .log()
		                                .log())
		            .expectNext(2, 2)
		            .verifyComplete();

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .log()
		                        .log())
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		StepVerifier.create(Mono.just(1)
		                        .log()
		                        .log())
		            .expectNext(1)
		            .verifyComplete();

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                                .log()
		                                .log())
		            .expectNext(1, 1)
		            .verifyComplete();

		Hooks.resetOnLastOperator();
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
		                        .log()
		                        .log())
		            .expectNext(4, 5, 6)
		            .verifyComplete();

		StepVerifier.create(Mono.just(1)
		                        .log()
		                        .log())
		            .expectNext(4)
		            .verifyComplete();

		StepVerifier.create(ParallelFlux.from(Mono.just(1), Mono.just(1))
		                                .log()
		                                .log())
		            .expectNext(6, 6)
		            .verifyComplete();

		Hooks.resetOnEachOperator();
	}

}
