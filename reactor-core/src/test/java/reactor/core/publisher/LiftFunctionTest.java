/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.awaitility.Awaitility;
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

	abstract static class Base {

		Publisher<Integer> liftOperator;
		String lifterName;

		void initLiftOperatorByLiftScannable(CorePublisher<Integer> source) {
			BiFunction<Scannable, CoreSubscriber<? super Integer>, CoreSubscriber<? super Integer>> lifter = (s, actual) -> actual;
			this.lifterName = lifter.toString();
			liftOperator = Operators.LiftFunction.liftScannable(null, lifter).apply(source);
		}

		void initLiftOperatorByLiftPublisher(CorePublisher<Integer> source) {
			BiFunction<Publisher, CoreSubscriber<? super Integer>, CoreSubscriber<? super Integer>> lifter = (s, actual) -> actual;
			this.lifterName = lifter.toString();
			liftOperator = Operators.LiftFunction.liftPublisher(null, lifter).apply(source);
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
	}

	@Nested
	class MonoLiftTest extends Base {

		Mono<Integer> source = Mono.just(1).hide();

		@Test
		void liftScannable() {
			initLiftOperatorByLiftScannable(source);

			lift(Mono.class, MonoLift.class);
		}

		@Test
		void scanLiftedAsScannable() {
			initLiftOperatorByLiftScannable(source);

			scanOperator(source, Integer.MAX_VALUE, Attr.RunStyle.SYNC);
		}

		@Test
		void liftPublisher() {
			initLiftOperatorByLiftPublisher(source);

			lift(Mono.class, MonoLift.class);
		}

		@Test
		void scanLiftedAsPublisher() {
			initLiftOperatorByLiftPublisher(source);

			scanOperator(source, Integer.MAX_VALUE, Attr.RunStyle.SYNC);
		}
	}

	@Nested
	class FluxLiftTest extends Base {

		Flux<Integer> source = Flux.just(1).hide();

		@Test
		void liftScannable() {
			initLiftOperatorByLiftScannable(source);

			lift(Flux.class, FluxLift.class);
		}

		@Test
		void scanLiftedAsScannable() {
			initLiftOperatorByLiftScannable(source);

			scanOperator(source, -1, Attr.RunStyle.SYNC);
		}

		@Test
		void liftPublisher() {
			initLiftOperatorByLiftPublisher(source);

			lift(Flux.class, FluxLift.class);
		}

		@Test
		void scanLiftedAsPublisher() {
			initLiftOperatorByLiftPublisher(source);

			scanOperator(source, -1, Attr.RunStyle.SYNC);
		}
	}

	@Nested
	class ParallelLiftTest extends Base {
		ParallelFlux<Integer> source = Flux.just(1).parallel(2).hide();

		@Test
		void liftScannable() {
			initLiftOperatorByLiftScannable(source);

			lift(ParallelFlux.class, ParallelLift.class);
		}

		@Test
		void scanLiftedAsScannable() {
			initLiftOperatorByLiftScannable(source);

			scanOperator(source, Queues.SMALL_BUFFER_SIZE, Attr.RunStyle.SYNC);
		}

		@Test
		void liftPublisher() {
			initLiftOperatorByLiftPublisher(source);

			lift(ParallelFlux.class, ParallelLift.class);
		}

		@Test
		void scanLiftedAsPublisher() {
			initLiftOperatorByLiftPublisher(source);

			scanOperator(source, Queues.SMALL_BUFFER_SIZE, Attr.RunStyle.SYNC);
		}
	}

	@Nested
	class ConnectableLiftTest extends Base {

		@Nested
		class Normal {
			ConnectableFlux<Integer> source = Flux.just(1).publish().hide();

			@Test
			void liftScannable() {
				initLiftOperatorByLiftScannable(source);

				lift(ConnectableFlux.class, ConnectableLift.class);
			}

			@Test
			void scanLiftedAsScannable() {
				initLiftOperatorByLiftScannable(source);

				scanOperator(source, Queues.SMALL_BUFFER_SIZE, Attr.RunStyle.SYNC);
			}

			@Test
			void liftPublisher() {
				initLiftOperatorByLiftPublisher(source);

				lift(ConnectableFlux.class, ConnectableLift.class);
			}

			@Test
			void scanLiftedAsPublisher() {
				initLiftOperatorByLiftPublisher(source);

				scanOperator(source, Queues.SMALL_BUFFER_SIZE, Attr.RunStyle.SYNC);
			}
		}

		@Nested
		class WithCancelSupport extends Base {

			//see https://github.com/reactor/reactor-core/issues/1860
			@Test
			public void liftConnectableFluxWithCancelSupport() {
				AtomicBoolean cancelSupportInvoked = new AtomicBoolean();
				ConnectableFlux<Integer> source = Flux.just(1)
						.publish(); //TODO hide if ConnectableFlux gets a hide function

				initLiftOperatorByLiftScannable(source);

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
	class MonoLiftFuseableTest extends Base {
		Mono<Integer> source = Mono.just(1);

		@Test
		void liftScannable() {
			initLiftOperatorByLiftScannable(source);

			liftFuseable(Mono.class, MonoLiftFuseable.class);
		}

		@Test
		void scanLiftedAsScannable() {
			initLiftOperatorByLiftScannable(source);

			scanOperator(source, Integer.MAX_VALUE, Attr.RunStyle.SYNC);
		}

		@Test
		void liftPublisher() {
			initLiftOperatorByLiftPublisher(source);

			liftFuseable(Mono.class, MonoLiftFuseable.class);
		}

		@Test
		void scanLiftedAsPublisher() {
			initLiftOperatorByLiftPublisher(source);

			scanOperator(source, Integer.MAX_VALUE, Attr.RunStyle.SYNC);
		}
	}

	@Nested
	class FluxLiftFuseableTest extends Base {
		Flux<Integer> source = Flux.just(1);

		@Test
		void liftScannable() {
			initLiftOperatorByLiftScannable(source);

			liftFuseable(Flux.class, FluxLiftFuseable.class);
		}

		@Test
		void scanLiftedAsScannable() {
			initLiftOperatorByLiftScannable(source);

			scanOperator(source, -1, Attr.RunStyle.SYNC);
		}

		@Test
		void liftPublisher() {
			initLiftOperatorByLiftPublisher(source);

			liftFuseable(Flux.class, FluxLiftFuseable.class);
		}

		@Test
		void scanLiftedAsPublisher() {
			initLiftOperatorByLiftPublisher(source);

			scanOperator(source, -1, Attr.RunStyle.SYNC);
		}
	}

	@Nested
	class ParallelLiftFuseableTest extends Base {
		ParallelFlux<Integer> source = Flux.just(1)
				.parallel(2)
				.reduce(() -> 1, (a, b) -> a);

		@Test
		void liftScannable() {
			initLiftOperatorByLiftScannable(source);

			liftFuseable(ParallelFlux.class, ParallelLiftFuseable.class);
		}

		@Test
		void scanLiftedAsScannable() {
			initLiftOperatorByLiftScannable(source);

			scanOperator(source, Integer.MAX_VALUE, Attr.RunStyle.SYNC);
		}

		@Test
		void liftPublisher() {
			initLiftOperatorByLiftPublisher(source);

			liftFuseable(ParallelFlux.class, ParallelLiftFuseable.class);
		}

		@Test
		void scanLiftedAsPublisher() {
			initLiftOperatorByLiftPublisher(source);

			scanOperator(source, Integer.MAX_VALUE, Attr.RunStyle.SYNC);
		}
	}

	@Nested
	class ConnectableLiftFuseableTest extends Base {

		@Nested
		class Normal {
			ConnectableFlux<Integer> source = Flux.just(1)
					.publish()
					.replay(2);

			@Test
			void liftScannable() {
				initLiftOperatorByLiftScannable(source);

				liftFuseable(ConnectableFlux.class, ConnectableLiftFuseable.class);
			}

			@Test
			void scanLiftedAsScannable() {
				initLiftOperatorByLiftScannable(source);

				scanOperator(source, 2, Attr.RunStyle.SYNC);
			}

			@Test
			void liftPublisher() {
				initLiftOperatorByLiftPublisher(source);

				liftFuseable(ConnectableFlux.class, ConnectableLiftFuseable.class);
			}

			@Test
			void scanLiftedAsPublisher() {
				initLiftOperatorByLiftPublisher(source);

				scanOperator(source, 2, Attr.RunStyle.SYNC);
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

				initLiftOperatorByLiftScannable(source);

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
		Flux<GroupedFlux<String, Integer>> sourceGroups = Flux.just(1)
				.groupBy(i -> "" + i);
		String groupLifterName;

		Operators.LiftFunction<Integer, Integer> liftFunction;

		@Test
		void liftScannable() {
			BiFunction<Scannable, CoreSubscriber<? super Integer>, CoreSubscriber<? super Integer>> groupedLifter =
					(s, actual) -> actual;
			groupLifterName = groupedLifter.toString();
			liftFunction = Operators.LiftFunction.liftScannable(null, groupedLifter);

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

		@Test
		void scanLiftedAsScannable() {
			BiFunction<Scannable, CoreSubscriber<? super Integer>, CoreSubscriber<? super Integer>> groupedLifter =
					(s, actual) -> actual;
			groupLifterName = groupedLifter.toString();
			liftFunction = Operators.LiftFunction.liftScannable(null, groupedLifter);

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

		@Test
		void liftPublisher() {
			BiFunction<Publisher, CoreSubscriber<? super Integer>, CoreSubscriber<? super Integer>> groupedLifter =
					(s, actual) -> actual;
			groupLifterName = groupedLifter.toString();
			liftFunction = Operators.LiftFunction.liftPublisher(null, groupedLifter);

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

		@Test
		void scanLiftedAsPublisher() {
			BiFunction<Publisher, CoreSubscriber<? super Integer>, CoreSubscriber<? super Integer>> groupedLifter =
					(s, actual) -> actual;
			groupLifterName = groupedLifter.toString();
			liftFunction = Operators.LiftFunction.liftPublisher(null, groupedLifter);

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
}
