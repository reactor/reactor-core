/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static reactor.core.Scannable.Attr;
import static reactor.core.Scannable.from;

public class LiftFunctionTest {

	Publisher<Integer> liftOperator;
	String lifterName;

	<T> Publisher<T> createPublisherAndApplyLiftScannable(CorePublisher<T> source) {
		BiFunction<Scannable, CoreSubscriber<? super T>, CoreSubscriber<? super T>> lifter = (s, actual) -> actual;
		this.lifterName = lifter.toString();
		Operators.LiftFunction<T, T> liftFunction =
				Operators.LiftFunction.liftScannable(null, lifter);
		return liftFunction.apply(source);
	}

	<T> Publisher<T> createPublisherAndApplyLiftPublisher(CorePublisher<T> source) {
		BiFunction<Publisher, CoreSubscriber<? super T>, CoreSubscriber<? super T>> lifter = (s, actual) -> actual;
		this.lifterName = lifter.toString();
		Operators.LiftFunction<T, T> liftFunction =
				Operators.LiftFunction.liftPublisher(null, lifter);
		return liftFunction.apply(source);
	}

	void lift(Class<?> publisher, Class<?> fluxPublisher) {
		assertThat(liftOperator)
				.isInstanceOf(publisher)
				.isExactlyInstanceOf(fluxPublisher);

		assertThatCode(() -> liftOperator.subscribe(new BaseSubscriber<Integer>() {
		}))
				.doesNotThrowAnyException();
	}

	void liftFuseable(Class<?> publisher, Class<?> fluxPublisher) {
		assertThat(liftOperator)
				.isInstanceOf(publisher)
				.isInstanceOf(Fuseable.class)
				.isExactlyInstanceOf(fluxPublisher);

		assertThatCode(() -> liftOperator.subscribe(new BaseSubscriber<Integer>() {
		}))
				.doesNotThrowAnyException();
	}

	void scanOperator(CorePublisher<?> source, int prefetch, Attr.RunStyle runStyle) {
		assertThat(from(liftOperator).scan(Attr.PARENT)).isSameAs(source);
		assertThat(from(liftOperator).scan(Attr.PREFETCH)).isEqualTo(prefetch);
		assertThat(from(liftOperator).scan(Attr.RUN_STYLE)).isSameAs(runStyle);
		assertThat(from(liftOperator).scan(Attr.LIFTER)).isEqualTo(lifterName);
	}

	@Nested
	class MonoLiftTest {

		abstract class Base {

			Mono<Integer> source = Mono.just(1).hide();

			@BeforeEach
			void init() {
				createMonoAndApply();
			}

			abstract  void createMonoAndApply();

			@Test
			void liftMono() {
				LiftFunctionTest.this.lift(Mono.class, MonoLift.class);
			}

			@Test
			void scanOperator() {
				LiftFunctionTest.this.scanOperator(source, Integer.MAX_VALUE, Attr.RunStyle.SYNC);
			}
		}

		@Nested
		class LiftScannable extends Base {
			@Override
			void createMonoAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftScannable(source);
			}
		}

		@Nested
		class LiftPublisher extends Base {
			@Override
			void createMonoAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftPublisher(source);
			}
		}
	}

	@Nested
	class FluxLiftTest {
		abstract class Base {

			Flux<Integer> source = Flux.just(1).hide();

			@BeforeEach
			void init() {
				createFluxAndApply();
			}

			abstract void createFluxAndApply();

			@Test
			void liftFlux() {
				LiftFunctionTest.this.lift(Flux.class, FluxLift.class);
			}

			@Test
			void scanOperator() {
				LiftFunctionTest.this.scanOperator(source, -1, Attr.RunStyle.SYNC);
			}
		}

		@Nested
		class LiftScannable extends Base {
			@Override
			void createFluxAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftScannable(source);
			}
		}

		@Nested
		class LiftPublisher extends Base {
			@Override
			void createFluxAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftPublisher(source);
			}
		}
	}

	@Nested
	class ParallelLiftTest {
		abstract class Base {
			ParallelFlux<Integer> source = Flux.just(1).parallel(2).hide();

			@BeforeEach
			void init() {
				createFluxAndApply();
			}

			abstract void createFluxAndApply();

			@Test
			void liftParallelFlux() {
				LiftFunctionTest.this.lift(ParallelFlux.class, ParallelLift.class);
			}

			@Test
			void scanOperator() {
				LiftFunctionTest.this.scanOperator(source, Queues.SMALL_BUFFER_SIZE, Attr.RunStyle.SYNC);
			}
		}

		@Nested
		class LiftScannable extends Base {
			@Override
			void createFluxAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftScannable(source);
			}
		}

		@Nested
		class LiftPublisher extends Base {
			@Override
			void createFluxAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftPublisher(source);
			}
		}
	}

	@Nested
	class ConnectableLiftTest {

		@Nested
		class Normal {
			abstract class Base {
				ConnectableFlux<Integer> source = Flux.just(1).publish().hide();

				@BeforeEach
				void init() {
					createFluxAndApply();
				}

				abstract void createFluxAndApply();

				@Test
				void liftConnectableFlux() {
					LiftFunctionTest.this.lift(ConnectableFlux.class, ConnectableLift.class);
				}

				@Test
				void scanOperator() {
					LiftFunctionTest.this.scanOperator(source, Queues.SMALL_BUFFER_SIZE, Attr.RunStyle.SYNC);
				}

			}

			@Nested
			class LiftScannable extends Base {
				@Override
				void createFluxAndApply() {
					liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftScannable(source);
				}
			}

			@Nested
			class LiftPublisher extends Base {
				@Override
				void createFluxAndApply() {
					liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftPublisher(source);
				}
			}
		}

		@Nested
		class WithCancelSupport {

			//see https://github.com/reactor/reactor-core/issues/1860
			@Test
			public void liftConnectableFluxWithCancelSupport() {
				AtomicBoolean cancelSupportInvoked = new AtomicBoolean();
				ConnectableFlux<Integer> source = Flux.just(1)
						.publish(); //TODO hide if ConnectableFlux gets a hide function

				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftScannable(source);

				assertThat(liftOperator)
						.isInstanceOf(ConnectableFlux.class)
						.isExactlyInstanceOf(ConnectableLift.class);

				@SuppressWarnings("unchecked")
				ConnectableLift<Integer, Integer> connectableLiftOperator = ((ConnectableLift<Integer, Integer>) liftOperator);

				connectableLiftOperator.connect(d -> cancelSupportInvoked.set(true));

				Awaitility.await().atMost(1, TimeUnit.SECONDS)
						.untilAsserted(() -> assertThat(cancelSupportInvoked).isTrue());
			}
		}
	}

	@Nested
	class GroupedLiftTest {

		@Disabled("GroupedFlux is always fuseable for now")
		@Test
		public void liftGroupedFlux() {
			Flux<GroupedFlux<String, Integer>> sourceGroups = Flux.just(1)
					.groupBy(i -> "" + i);

			Operators.LiftFunction<Integer, Integer> liftFunction =
					Operators.LiftFunction.liftScannable(null, (s, actual) -> actual);

			sourceGroups.map(g -> liftFunction.apply(g)) //TODO hide if GroupedFlux gets a proper hide() function
					.doOnNext(liftOperator -> assertThat(liftOperator)
							.isInstanceOf(GroupedFlux.class)
							.isExactlyInstanceOf(GroupedLift.class))
					.blockLast();
		}

	}

	@Nested
	class MonoLiftFuseableTest {
		abstract class Base {
			Mono<Integer> source = Mono.just(1);

			@BeforeEach
			void init() {
				createMonoAndApply();
			}

			abstract void createMonoAndApply();

			@Test
			void liftMonoFuseable() {
				LiftFunctionTest.this.liftFuseable(Mono.class, MonoLiftFuseable.class);
			}

			@Test
			void scanOperator() {
				LiftFunctionTest.this.scanOperator(source, Integer.MAX_VALUE, Attr.RunStyle.SYNC);
			}

		}

		@Nested
		class LiftScannable extends Base {
			@Override
			void createMonoAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftScannable(source);
			}
		}

		@Nested
		class LiftPublisher extends Base {
			@Override
			void createMonoAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftPublisher(source);
			}
		}

	}

	@Nested
	class FluxLiftFuseableTest {
		abstract class Base {
			Flux<Integer> source = Flux.just(1);

			@BeforeEach
			void init() {
				createFluxAndApply();
			}

			abstract void createFluxAndApply();

			@Test
			void liftFluxFuseable() {
				LiftFunctionTest.this.lift(Flux.class, FluxLiftFuseable.class);
			}

			@Test
			void scanOperator() {
				LiftFunctionTest.this.scanOperator(source, -1, Attr.RunStyle.SYNC);
			}
		}

		@Nested
		class LiftScannable extends Base {
			@Override
			void createFluxAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftScannable(source);
			}
		}

		@Nested
		class LiftPublisher extends Base {
			@Override
			void createFluxAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftPublisher(source);
			}
		}
	}

	@Nested
	class ParallelLiftFuseableTest {
		abstract class Base {
			ParallelFlux<Integer> source = Flux.just(1)
					.parallel(2)
					.reduce(() -> 1, (a, b) -> a);

			@BeforeEach
			void init() {
				createFluxAndApply();
			}

			abstract void createFluxAndApply();

			@Test
			void liftParallelFluxFuseable() {
				LiftFunctionTest.this.lift(ParallelFlux.class, ParallelLiftFuseable.class);
			}

			@Test
			void scanOperator() {
				LiftFunctionTest.this.scanOperator(source, Integer.MAX_VALUE, Attr.RunStyle.SYNC);
			}
		}

		@Nested
		class LiftScannable extends Base {
			@Override
			void createFluxAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftScannable(source);
			}
		}

		@Nested
		class LiftPublisher extends Base {
			@Override
			void createFluxAndApply() {
				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftPublisher(source);
			}
		}
	}

	@Nested
	class ConnectableLiftFuseableTest {

		@Nested
		class Normal {
			abstract class Base {
				ConnectableFlux<Integer> source = Flux.just(1)
						.publish()
						.replay(2);

				@BeforeEach
				void init() {
					createFluxAndApply();
				}

				abstract void createFluxAndApply();

				@Test
				void liftConnectableFluxFuseable() {
					LiftFunctionTest.this.liftFuseable(ConnectableFlux.class, ConnectableLiftFuseable.class);
				}

				@Test
				void scanOperator() {
					LiftFunctionTest.this.scanOperator(source, 2, Attr.RunStyle.SYNC);
				}
			}

			@Nested
			class LiftScannable extends Base {
				@Override
				void createFluxAndApply() {
					liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftScannable(source);
				}
			}

			@Nested
			class LiftPublisher extends Base {
				@Override
				void createFluxAndApply() {
					liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftPublisher(source);
				}
			}
		}

		@Nested
		class WithCancelSupport {

			//see https://github.com/reactor/reactor-core/issues/1860
			@Test
			void liftConnectableFluxFuseableWithCancelSupport() {
				AtomicBoolean cancelSupportInvoked = new AtomicBoolean();

				ConnectableFlux<Integer> source = Flux.just(1)
						.replay();

				liftOperator = LiftFunctionTest.this.createPublisherAndApplyLiftScannable(source);

				assertThat(liftOperator)
						.isExactlyInstanceOf(ConnectableLiftFuseable.class);

				@SuppressWarnings("unchecked")
				ConnectableLiftFuseable<Integer, Integer> connectableLifOperator = (ConnectableLiftFuseable<Integer, Integer>) liftOperator;
				connectableLifOperator.connect(d -> cancelSupportInvoked.set(true));

				Awaitility.await().atMost(1, TimeUnit.SECONDS)
						.untilAsserted(() -> assertThat(cancelSupportInvoked).isTrue());
			}
		}
	}

	@Nested
	class GroupedLiftFuseableTest {
		abstract class Base {
			Flux<GroupedFlux<String, Integer>> sourceGroups = Flux.just(1)
					.groupBy(i -> "" + i);
			String groupLifterName;

			Operators.LiftFunction<Integer, Integer> liftFunction;

			@BeforeEach
			void init() {
				groupLifterName = null;
				liftFunction = null;
				createFluxAndApply();
			}

			abstract void createFluxAndApply();

			@Test
			public void liftGroupedFluxFuseable() {
				Operators.LiftFunction<Integer, Integer> liftFunction =
						Operators.LiftFunction.liftScannable(null, (s, actual) -> actual);

				sourceGroups.map(liftFunction)
						.doOnNext(liftOperator -> assertThat(liftOperator)
								.isInstanceOf(GroupedFlux.class)
								.isInstanceOf(Fuseable.class)
								.isExactlyInstanceOf(GroupedLiftFuseable.class))
						.blockLast();
			}

			@Test
			public void scanOperator() {
				sourceGroups.map(g -> {
					Publisher<Integer> liftOperator = liftFunction.apply(g);

					assertThat(from(liftOperator).scan(Attr.PARENT)).isSameAs(g);
					assertThat(from(liftOperator).scan(Attr.PREFETCH)).isSameAs(g.getPrefetch());
					assertThat(from(liftOperator).scan(Attr.RUN_STYLE))
							.isSameAs(Attr.RunStyle.SYNC)
							.isSameAs(from(g).scan(Attr.RUN_STYLE));
					assertThat(from(liftOperator).scan(Attr.LIFTER)).isEqualTo(groupLifterName);

					return liftOperator;
				})
				.blockLast();
			}
		}

		@Nested
		class LiftScannable extends Base {
			@Override
			void createFluxAndApply() {
				BiFunction<Scannable, CoreSubscriber<? super Integer>, CoreSubscriber<? super Integer>> groupedLifter =
						(s, actual) -> actual;
				groupLifterName = groupedLifter.toString();
				liftFunction = Operators.LiftFunction.liftScannable(null, groupedLifter);
			}
		}

		@Nested
		class LiftPublisher extends Base {
			@Override
			void createFluxAndApply() {
				final BiFunction<Publisher, CoreSubscriber<? super Integer>, CoreSubscriber<? super Integer>> groupedLifter = (s, actual) -> actual;
				groupLifterName = groupedLifter.toString();
				liftFunction = Operators.LiftFunction.liftPublisher(null, groupedLifter);
			}
		}
	}
}
