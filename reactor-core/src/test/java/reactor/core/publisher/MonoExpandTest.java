/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoExpandTest {
	Function<Integer, Publisher<Integer>> countDown =
			v -> v == 0 ? Flux.empty() : Flux.just(v - 1);


	@Test
	public void recursiveCountdown() {
		StepVerifier.create(Mono.just(10)
		                        .expand(countDown))
		            .expectNext(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
		            .verifyComplete();
	}

	@Test
	public void recursiveCountdownDepth() {
		StepVerifier.create(Mono.just(10)
		                        .expandDeep(countDown))
		            .expectNext(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
		            .verifyComplete();
	}

	@Test
	public void error() {
		StepVerifier.create(Mono.<Integer>error(new IllegalStateException("boom"))
				.expand(countDown))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void errorDepth() {
		StepVerifier.create(Mono.<Integer>error(new IllegalStateException("boom"))
				.expandDeep(countDown))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void empty() {
		StepVerifier.create(Mono.<Integer>empty()
				.expand(countDown))
		            .verifyComplete();
	}

	@Test
	public void emptyDepth() {
		StepVerifier.create(Mono.<Integer>empty()
				.expandDeep(countDown))
		            .verifyComplete();
	}

	@Test
	public void recursiveCountdownLoop() {
		for (int i = 0; i < 1000; i = (i < 100 ? i + 1 : i + 50)) {
			String tag = "i = " + i + ", strategy = breadth";

			List<Integer> list = new ArrayList<>();

			StepVerifier.create(Mono.just(i)
			                        .expand(countDown))
			            .expectSubscription()
			            .recordWith(() -> list)
			            .expectNextCount(i + 1)
			            .as(tag)
			            .verifyComplete();

			for (int j = 0; j <= i; j++) {
				assertThat(list.get(j).intValue())
						.as("%s, %s", tag, list)
						.isEqualTo(i - j);
			}
		}
	}

	@Test
	public void recursiveCountdownLoopDepth() {
		for (int i = 0; i < 1000; i = (i < 100 ? i + 1 : i + 50)) {
			String tag = "i = " + i + ", strategy = depth";

			List<Integer> list = new ArrayList<>();

			StepVerifier.create(Mono.just(i)
			                        .expandDeep(countDown))
			            .expectSubscription()
			            .recordWith(() -> list)
			            .expectNextCount(i + 1)
			            .as(tag)
			            .verifyComplete();

			for (int j = 0; j <= i; j++) {
				assertThat(list.get(j).intValue())
						.as("%s, %s", tag, list)
						.isEqualTo(i - j);
			}
		}
	}

	@Test
	public void recursiveCountdownTake() {
		StepVerifier.create(Mono.just(10)
		                        .expand(countDown)
		                        .take(5)
		)
		            .expectNext(10, 9, 8, 7, 6)
		            .verifyComplete();
	}

	@Test
	public void recursiveCountdownTakeDepth() {
		StepVerifier.create(Mono.just(10)
		                        .expandDeep(countDown)
		                        .take(5)
		)
		            .expectNext(10, 9, 8, 7, 6)
		            .verifyComplete();
	}

	@Test
	public void recursiveCountdownBackpressure() {
		StepVerifier.create(Mono.just(10)
		                        .expand(countDown),
				StepVerifierOptions.create()
				                   .initialRequest(0)
				                   .checkUnderRequesting(false))
		            .thenRequest(1)
		            .expectNext(10)
		            .thenRequest(3)
		            .expectNext(9, 8, 7)
		            .thenRequest(4)
		            .expectNext(6, 5, 4, 3)
		            .thenRequest(3)
		            .expectNext(2, 1, 0)
		            .verifyComplete();
	}

	@Test
	public void recursiveCountdownBackpressureDepth() {
		StepVerifier.create(Mono.just(10)
		                        .expandDeep(countDown),
				StepVerifierOptions.create()
				                   .initialRequest(0)
				                   .checkUnderRequesting(false))
		            .thenRequest(1)
		            .expectNext(10)
		            .thenRequest(3)
		            .expectNext(9, 8, 7)
		            .thenRequest(4)
		            .expectNext(6, 5, 4, 3)
		            .thenRequest(3)
		            .expectNext(2, 1, 0)
		            .verifyComplete();
	}

	@Test
	public void expanderThrows() {
		StepVerifier.create(Mono.just(10)
		                        .expand(v -> {
			                        throw new IllegalStateException("boom");
		                        }))
		            .expectNext(10)
		            .verifyErrorMessage("boom");
	}

	@Test
	public void expanderThrowsDepth() {
		StepVerifier.create(Mono.just(10)
		                        .expandDeep(v -> {
			                        throw new IllegalStateException("boom");
		                        }))
		            .expectNext(10)
		            .verifyErrorMessage("boom");
	}

	@Test
	public void expanderReturnsNull() {
		StepVerifier.create(Mono.just(10)
		                        .expand(v -> null))
		            .expectNext(10)
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void expanderReturnsNullDepth() {
		StepVerifier.create(Mono.just(10)
		                        .expandDeep(v -> null))
		            .expectNext(10)
		            .verifyError(NullPointerException.class);
	}

	FluxExpandTest.Node createTest() {
		return new FluxExpandTest.Node("root",
				new FluxExpandTest.Node("1",
						new FluxExpandTest.Node("11")
				),
				new FluxExpandTest.Node("2",
						new FluxExpandTest.Node("21"),
						new FluxExpandTest.Node("22",
								new FluxExpandTest.Node("221")
						)
				),
				new FluxExpandTest.Node("3",
						new FluxExpandTest.Node("31"),
						new FluxExpandTest.Node("32",
								new FluxExpandTest.Node("321")
						),
						new FluxExpandTest.Node("33",
								new FluxExpandTest.Node("331"),
								new FluxExpandTest.Node("332",
										new FluxExpandTest.Node("3321")
								)
						)
				),
				new FluxExpandTest.Node("4",
						new FluxExpandTest.Node("41"),
						new FluxExpandTest.Node("42",
								new FluxExpandTest.Node("421")
						),
						new FluxExpandTest.Node("43",
								new FluxExpandTest.Node("431"),
								new FluxExpandTest.Node("432",
										new FluxExpandTest.Node("4321")
								)
						),
						new FluxExpandTest.Node("44",
								new FluxExpandTest.Node("441"),
								new FluxExpandTest.Node("442",
										new FluxExpandTest.Node("4421")
								),
								new FluxExpandTest.Node("443",
										new FluxExpandTest.Node("4431"),
										new FluxExpandTest.Node("4432")
								)
						)
				)
		);
	}

	@Test
	@Timeout(5)
	public void depthFirst() {
		FluxExpandTest.Node root = createTest();

		StepVerifier.create(Mono.just(root)
		                        .expandDeep(v -> Flux.fromIterable(v.children))
		                        .map(v -> v.name))
		            .expectNext(
				            "root",
				            "1", "11",
				            "2", "21", "22", "221",
				            "3", "31", "32", "321", "33", "331", "332", "3321",
				            "4", "41", "42", "421", "43", "431", "432", "4321",
				            "44", "441", "442", "4421", "443", "4431", "4432"
		            )
		            .verifyComplete();
	}

	@Test
	public void depthFirstAsync() {
		FluxExpandTest.Node root = createTest();

		StepVerifier.create(Mono.just(root)
		                        .expandDeep(v -> Flux.fromIterable(v.children)
		                                         .subscribeOn(Schedulers.boundedElastic()))
		                        .map(v -> v.name))
		            .expectNext(
				            "root",
				            "1", "11",
				            "2", "21", "22", "221",
				            "3", "31", "32", "321", "33", "331", "332", "3321",
				            "4", "41", "42", "421", "43", "431", "432", "4321",
				            "44", "441", "442", "4421", "443", "4431", "4432"
		            )
		            .expectComplete()
		            .verify(Duration.ofSeconds(5));
	}

	@Test
	@Timeout(5)
	public void breadthFirst() {
		FluxExpandTest.Node root = createTest();

		StepVerifier.create(Mono.just(root)
		                        .expand(v -> Flux.fromIterable(v.children))
		                        .map(v -> v.name))
		            .expectNext(
				            "root",
				            "1", "2", "3", "4",
				            "11", "21", "22", "31", "32", "33", "41", "42", "43", "44",
				            "221", "321", "331", "332", "421", "431", "432", "441", "442", "443",
				            "3321", "4321", "4421", "4431", "4432"
		            )
		            .verifyComplete();
	}

	@Test
	public void breadthFirstAsync() {
		FluxExpandTest.Node root = createTest();

		StepVerifier.create(Mono.just(root)
		                        .expand(v -> Flux.fromIterable(v.children).subscribeOn(Schedulers.boundedElastic()))
		                        .map(v -> v.name))
		            .expectNext(
				            "root",
				            "1", "2", "3", "4",
				            "11", "21", "22", "31", "32", "33", "41", "42", "43", "44",
				            "221", "321", "331", "332", "421", "431", "432", "441", "442", "443",
				            "3321", "4321", "4421", "4431", "4432"
		            )
		            .expectComplete()
		            .verify(Duration.ofSeconds(5));
	}

	@Test
	public void depthFirstCancel() {
		final TestPublisher<Integer> pp = TestPublisher.create();
		final AssertSubscriber<Integer> ts = AssertSubscriber.create();
		CoreSubscriber<Integer> s = new CoreSubscriber<Integer>() {

			Subscription upstream;

			@Override
			public void onSubscribe(Subscription s) {
				upstream = s;
				ts.onSubscribe(s);
			}

			@Override
			public void onNext(Integer t) {
				ts.onNext(t);
				upstream.cancel();
				upstream.request(1);
				onComplete();
			}

			@Override
			public void onError(Throwable t) {
				ts.onError(t);
			}

			@Override
			public void onComplete() {
				ts.onComplete();
			}
		};

		Mono.just(1)
		    .expandDeep(it -> pp)
		    .subscribe(s);

		pp.assertNoSubscribers();

		ts.assertValues(1);
	}

	@Test
	public void depthCancelRace() {
		for (int i = 0; i < 1000; i++) {
			final AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

			Mono.just(0)
			    .expandDeep(countDown)
			    .subscribe(ts);

			Runnable r1 = () -> ts.request(1);
			Runnable r2 = ts::cancel;

			RaceTestUtils.race(r1, r2, Schedulers.single());
		}
	}

	@Test
	public void depthEmitCancelRace() {
		for (int i = 0; i < 1000; i++) {

			final TestPublisher<Integer> pp = TestPublisher.create();

			final AssertSubscriber<Integer> ts = AssertSubscriber.create(1);

			Mono.just(0)
			    .expandDeep(it -> pp)
			    .subscribe(ts);

			Runnable r1 = () -> pp.next(1);
			Runnable r2 = ts::cancel;

			RaceTestUtils.race(r1, r2, Schedulers.single());
		}
	}

	@Test
	public void depthCompleteCancelRace() {
		for (int i = 0; i < 1000; i++) {

			final TestPublisher<Integer> pp = TestPublisher.create();

			final AssertSubscriber<Integer> ts = AssertSubscriber.create(1);
			Mono.just(0)
			    .expandDeep(it -> pp)
			    .subscribe(ts);

			Runnable r1 = pp::complete;
			Runnable r2 = ts::cancel;

			RaceTestUtils.race(r1, r2, Schedulers.single());
		}
	}

	@Test
	public void depthCancelRace2() throws Exception {
		for (int i = 0; i < 1000; i++) {

			final TestPublisher<Integer> pp = TestPublisher.create();

			Flux<Integer> source = Mono.just(0)
			                           .expandDeep(it -> pp);

			final CountDownLatch cdl = new CountDownLatch(1);

			AssertSubscriber<Integer> ts = new AssertSubscriber<Integer>() {

				final AtomicInteger sync = new AtomicInteger(2);

				@Override
				public void onNext(Integer t) {
					super.onNext(t);
					Schedulers.single().schedule(() -> {
						if (sync.decrementAndGet() != 0) {
							while (sync.get() != 0) { }
						}
						cancel();
						cdl.countDown();
					});
					if (sync.decrementAndGet() != 0) {
						while (sync.get() != 0) { }
					}
				}
			};

			source.subscribe(ts);

			assertThat(cdl.await(5, TimeUnit.SECONDS)).as("runs under 5s").isTrue();
		}
	}

	static final FluxExpandTest.Node ROOT = new FluxExpandTest.Node("A",
			new FluxExpandTest.Node("AA",
					new FluxExpandTest.Node("aa1")),
			new FluxExpandTest.Node("AB",
					new FluxExpandTest.Node("ab1")),
			new FluxExpandTest.Node("a1")
	);

	@Test
	public void javadocExampleBreadthFirst() {
		List<String> breadthFirstExpected = Arrays.asList(
				"A",
				"AA",
				"AB",
				"a1",
				"aa1",
				"ab1");

		StepVerifier.create(
				Mono.just(ROOT)
				    .expand(v -> Flux.fromIterable(v.children))
				    .map(n -> n.name))
		            .expectNextSequence(breadthFirstExpected)
		            .verifyComplete();
	}

	@Test
	public void javadocExampleDepthFirst() {
		List<String> depthFirstExpected = Arrays.asList(
				"A",
				"AA",
				"aa1",
				"AB",
				"ab1",
				"a1");

		StepVerifier.create(
				Mono.just(ROOT)
				    .expandDeep(v -> Flux.fromIterable(v.children))
				    .map(n -> n.name))
		            .expectNextSequence(depthFirstExpected)
		            .verifyComplete();
	}

	@Test
	public void scanOperator(){
	    MonoExpand<Integer> test = new MonoExpand<>(Mono.just(10), countDown, false, 10);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
