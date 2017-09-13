/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Schedulers;
import reactor.test.RaceTestUtils;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxExpandTest {

	Function<Integer, Publisher<Integer>> countDown =
			v -> v == 0 ? Flux.empty() : Flux.just(v - 1);

	@Test
	public void recursiveCountdownDepth() {
		StepVerifier.create(Flux.just(10)
		                        .expand(countDown, ExpandStrategy.DEPTH_FIRST))
		            .expectNext(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
		            .verifyComplete();
	}

	@Test
	public void recursiveCountdownDefault() {
		StepVerifier.create(Flux.just(10)
		                        .expand(countDown))
		            .expectNext(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
		            .verifyComplete();
	}

	@Test
	public void recursiveCountdownBreadth() {
		StepVerifier.create(Flux.just(10)
		                        .expand(countDown, ExpandStrategy.BREADTH_FIRST))
		            .expectNext(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
		            .verifyComplete();
	}

	@Test
	public void error() {
		for (ExpandStrategy strategy : ExpandStrategy.values()) {
			StepVerifier.create(Flux.<Integer>error(new IllegalStateException("boom"))
					.expand(countDown, strategy))
			            .verifyErrorMessage("boom");
		}
	}

	@Test
	public void empty() {
		for (ExpandStrategy strategy : ExpandStrategy.values()) {
			StepVerifier.create(Flux.<Integer>empty()
					.expand(countDown, strategy))
			            .verifyComplete();
		}
	}

	@Test
	public void recursiveCountdown() {
		for (ExpandStrategy strategy : ExpandStrategy.values()) {
			for (int i = 0; i < 1000; i = (i < 100 ? i + 1 : i + 50)) {
				String tag = "i = " + i + ", strategy = " + strategy;

				List<Integer> list = new ArrayList<>();

				StepVerifier.create(Flux.just(i)
				                        .expand(countDown, strategy))
				            .expectSubscription()
				            .recordWith(() -> list)
				            .expectNextCount(i + 1)
				            .as(tag)
				            .verifyComplete();

				for (int j = 0; j <= i; j++) {
					assertThat(list.get(j).intValue())
							.as(tag + ", " + list)
							.isEqualTo(i - j);
				}
			}
		}
	}

	@Test
	public void recursiveCountdownTake() {
		for (ExpandStrategy strategy : ExpandStrategy.values()) {
			StepVerifier.create(Flux.just(10)
			                        .expand(countDown, strategy)
			                        .take(5)
			)
			            .expectNext(10, 9, 8, 7, 6)
			            .as(strategy.toString()) //TODO replace with a global test description?
			            .verifyComplete();
		}
	}

	@Test
	public void recursiveCountdownBackpressure() {
		for (ExpandStrategy strategy : ExpandStrategy.values()) {
			StepVerifier.create(Flux.just(10)
			                        .expand(countDown, strategy),
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
	}

	@Test
	public void expanderThrows() {
		for (ExpandStrategy strategy : ExpandStrategy.values()) {
			StepVerifier.create(Flux.just(10)
			                        .expand(v -> {
				                        throw new IllegalStateException("boom");
			                        }, strategy))
			            .expectNext(10)
			            .as(strategy.toString()) //TODO replace with global test description
			            .verifyErrorMessage("boom");
		}
	}

	@Test
	public void expanderReturnsNull() {
		for (ExpandStrategy strategy : ExpandStrategy.values()) {
			StepVerifier.create(Flux.just(10)
			                        .expand(v -> null, strategy))
			            .expectNext(10)
			            .as(strategy.toString())
			            .verifyError(NullPointerException.class);
		}
	}

	static final class Node {
		final String name;
		final List<Node> children;

		Node(String name, Node... nodes) {
			this.name = name;
			this.children = new ArrayList<>();
			children.addAll(Arrays.asList(nodes));
		}

		@Override
		public String toString() {
			return name;
		}
	}

	Node createTest() {
		return new Node("root",
				new Node("1",
						new Node("11")
				),
				new Node("2",
						new Node("21"),
						new Node("22",
								new Node("221")
						)
				),
				new Node("3",
						new Node("31"),
						new Node("32",
								new Node("321")
						),
						new Node("33",
								new Node("331"),
								new Node("332",
										new Node("3321")
								)
						)
				),
				new Node("4",
						new Node("41"),
						new Node("42",
								new Node("421")
						),
						new Node("43",
								new Node("431"),
								new Node("432",
										new Node("4321")
								)
						),
						new Node("44",
								new Node("441"),
								new Node("442",
										new Node("4421")
								),
								new Node("443",
										new Node("4431"),
										new Node("4432")
								)
						)
				)
		);
	}

	@Test(timeout = 5000)
	public void depthFirst() {
		Node root = createTest();

		StepVerifier.create(Flux.just(root)
		                        .expand(v -> Flux.fromIterable(v.children), ExpandStrategy.DEPTH_FIRST)
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
		Node root = createTest();

		StepVerifier.create(Flux.just(root)
		                        .expand(v -> Flux.fromIterable(v.children)
		                                         .subscribeOn(Schedulers.elastic()),
				                        ExpandStrategy.DEPTH_FIRST)
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

	@Test(timeout = 5000)
	public void breadthFirst() {
		Node root = createTest();

		StepVerifier.create(Flux.just(root)
		                        .expand(v -> Flux.fromIterable(v.children), ExpandStrategy.BREADTH_FIRST)
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
		Node root = createTest();

		StepVerifier.create(Flux.just(root)
		                        .expand(v -> Flux.fromIterable(v.children).subscribeOn(Schedulers.elastic()), ExpandStrategy.BREADTH_FIRST)
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

		Flux.just(1)
		    .expand(it -> pp, ExpandStrategy.DEPTH_FIRST)
		    .subscribe(s);

		pp.assertNoSubscribers();

		ts.assertValues(1);
	}

	@Test
	public void depthCancelRace() {
		for (int i = 0; i < 1000; i++) {
			final AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

			Flux.just(0)
			    .expand(countDown, ExpandStrategy.DEPTH_FIRST)
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

			Flux.just(0)
			    .expand(it -> pp, ExpandStrategy.DEPTH_FIRST)
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
			Flux.just(0)
			    .expand(it -> pp, ExpandStrategy.DEPTH_FIRST)
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

			Flux<Integer> source = Flux.just(0)
			                           .expand(it -> pp, ExpandStrategy.DEPTH_FIRST);

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

	static final Node ROOT_A = new Node("A",
			new Node("AA",
					new Node("aa1")),
			new Node("AB",
					new Node("ab1")),
			new Node("a1")
	);

	static final Node ROOT_B = new Node("B",
			new Node("BA",
					new Node("ba1")),
			new Node("BB",
					new Node("bb1")),
			new Node("BC",
					new Node("bc1"),
					new Node("bc2")),
			new Node("b1"),
			new Node("b2")
	);

	@Test
	public void javadocExampleBreadthFirst() {
		List<String> breadthFirstExpected = Arrays.asList(
				"A",
				"B",
				"AA",
				"AB",
				"a1",
				"BA",
				"BB",
				"BC",
				"b1",
				"b2",
				"aa1",
				"ab1",
				"ba1",
				"bb1",
				"bc1",
				"bc2");

		StepVerifier.create(
				Flux.just(ROOT_A, ROOT_B)
				    .expand(v -> Flux.fromIterable(v.children),
						    ExpandStrategy.BREADTH_FIRST)
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
				"a1",
				"B",
				"BA",
				"ba1",
				"BB",
				"bb1",
				"BC",
				"bc1",
				"bc2",
				"b1",
				"b2");

		StepVerifier.create(
				Flux.just(ROOT_A, ROOT_B)
				    .expand(v -> Flux.fromIterable(v.children),
						    ExpandStrategy.DEPTH_FIRST)
		            .map(n -> n.name))
		            .expectNextSequence(depthFirstExpected)
		            .verifyComplete();
	}

	@Test
	public void defaultIsBreadthFirst() {
		ExpandStrategy expectedStrategy = ExpandStrategy.BREADTH_FIRST;

		List<String> explicitStrategy = Flux.just(ROOT_A, ROOT_B)
		                                    .expand(v -> Flux.fromIterable(v.children),
				                                    expectedStrategy)
		                                    .map(n -> n.name)
		                                    .collectList()
		                                    .block();

		List<String> defaultStrategy = Flux.just(ROOT_A, ROOT_B)
		                                    .expand(v -> Flux.fromIterable(v.children))
		                                    .map(n -> n.name)
		                                    .collectList()
		                                    .block();

		assertThat(defaultStrategy)
				.withFailMessage("default is not breadth first. Expected:\n%s\nGot:\n%s", explicitStrategy, defaultStrategy)
				.containsExactlyElementsOf(explicitStrategy);
	}
}
