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

package reactor.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class ScannableTest {

	static final Scannable scannable = key -> {
		if (key == Scannable.Attr.BUFFERED) return 1;
		if (key == Scannable.Attr.TERMINATED) return true;
		if (key == Scannable.Attr.PARENT) return null;
		if (key == Scannable.Attr.ACTUAL)
			return (Scannable) k -> (Scannable) k2 -> null;

		return null;
	};

	@Test
	public void unavailableScan() {
		assertThat(Scannable.from("nothing")).isEqualTo(Scannable.Attr.UNAVAILABLE_SCAN);
		assertThat(Scannable.from("nothing").isScanAvailable()).isFalse();
		assertThat(Scannable.from("nothing").inners().count()).isEqualTo(0);
		assertThat(Scannable.from("nothing").parents().count()).isEqualTo(0);
		assertThat(Scannable.from("nothing").actuals().count()).isEqualTo(0);
		assertThat(Scannable.from("nothing").scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(Scannable.from("nothing").scanOrDefault(Scannable.Attr.BUFFERED, 0)).isEqualTo(0);
		assertThat(Scannable.from("nothing").scan(Scannable.Attr.ACTUAL)).isNull();
	}

	@Test
	public void meaningfulDefaults() {
		Scannable emptyScannable = key -> null;

		assertThat(emptyScannable.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		assertThat(emptyScannable.scan(Scannable.Attr.LARGE_BUFFERED)).isNull();
		assertThat(emptyScannable.scan(Scannable.Attr.CAPACITY)).isEqualTo(0);
		assertThat(emptyScannable.scan(Scannable.Attr.PREFETCH)).isEqualTo(0);

		assertThat(emptyScannable.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);

		assertThat(emptyScannable.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(emptyScannable.scan(Scannable.Attr.DELAY_ERROR)).isFalse();
		assertThat(emptyScannable.scan(Scannable.Attr.TERMINATED)).isFalse();

		assertThat(emptyScannable.scan(Scannable.Attr.ERROR)).isNull();

		assertThat(emptyScannable.scan(Scannable.Attr.ACTUAL)).isNull();
		assertThat(emptyScannable.scan(Scannable.Attr.PARENT)).isNull();

		assertThat(emptyScannable.scan(Scannable.Attr.TAGS)).isNull();
		assertThat(emptyScannable.scan(Scannable.Attr.NAME)).isNull();
	}

	@Test
	public void scanOrDefaultOverridesGlobalDefault() {
		Scannable emptyScannable = key -> null;

		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.BUFFERED, 123)).isEqualTo(123); //global 0
		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.CAPACITY, 123)).isEqualTo(123); //global 0
		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.PREFETCH, 123)).isEqualTo(123); //global 0

		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.LARGE_BUFFERED, 123L)).isEqualTo(123L); //global null
		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM, 123L)).isEqualTo(123L); //global 0

		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.CANCELLED, true)).isTrue(); //global false
		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.DELAY_ERROR, true)).isTrue(); //global false
		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.TERMINATED, true)).isTrue(); //global false

		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.ERROR, new IllegalStateException())).isInstanceOf(IllegalStateException.class); //global null

		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.ACTUAL, Scannable.Attr.NULL_SCAN)).isSameAs(Scannable.Attr.NULL_SCAN); //global null
		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.PARENT, Scannable.Attr.NULL_SCAN)).isSameAs(Scannable.Attr.NULL_SCAN); // global null

		List<Tuple2<String, String>> tags = Collections.singletonList(Tuples.of("some", "key"));
		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.TAGS, tags.stream())).containsExactlyElementsOf(tags); //global null
		assertThat(emptyScannable.scanOrDefault(Scannable.Attr.NAME, "SomeName")).isEqualTo("SomeName"); // global null
	}

	@Test
	public void availableScan() {
		assertThat(Scannable.from(scannable)).isEqualTo(scannable);
		assertThat(Scannable.from(scannable).isScanAvailable()).isTrue();
		assertThat(Scannable.from(scannable).inners().count()).isEqualTo(0);
		assertThat(Scannable.from(scannable).parents().count()).isEqualTo(0);
		assertThat(Scannable.from(scannable).actuals().count()).isEqualTo(2);
		assertThat(Scannable.from(scannable).scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(Scannable.from(scannable).scanOrDefault(Scannable.Attr.BUFFERED, 0)).isEqualTo(1);
		assertThat(Scannable.from(scannable).scan(Scannable.Attr.ACTUAL)).isEqualTo(scannable.actuals().findFirst().get());
	}

	@Test
	public void nullScan() {
		assertThat(Scannable.from(null))
				.isNotNull()
				.isSameAs(Scannable.Attr.NULL_SCAN);
	}

	@Test
	public void namedFluxTest() {
		Flux<Integer> named1 =
				Flux.range(1, 10)
				    .name("100s");

		Flux<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .name("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}


	@Test
	public void namedHideFluxTest() {
		Flux<Integer> named1 =
				Flux.range(1, 10)
				    .hide()
				    .name("100s");

		Flux<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .name("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}

	@Test
	public void namedOverridenFluxTest() {
		Flux<Integer> named1 =
				Flux.range(1, 10)
				    .name("1s")
				    .name("100s");

		Flux<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .name("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}

	@Test
	public void namedOverridenHideFluxTest() {
		Flux<Integer> named1 =
				Flux.range(1, 10)
				    .hide()
				    .name("1s")
				    .name("100s");

		Flux<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .name("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}

	@Test
	public void scannableNameDefaultsToToString() {
		final Flux<Integer> flux = Flux.range(1, 10)
		                               .map(i -> i + 10);

		assertThat(Scannable.from(flux).name())
				.isEqualTo(Scannable.from(flux).stepName())
				.isEqualTo("map");
	}

	@Test
	public void taggedFluxTest() {
		Flux<Integer> tagged1 =
				Flux.range(1, 10)
				    .tag("1", "One");


		Flux<Integer> tagged2 = tagged1.filter(i -> i % 3 == 0)
		                               .tag("2", "Two")
		                               .hide();

		final Stream<Tuple2<String, String>> scannedTags1 = Scannable.from(tagged1).tags();
		assertThat(scannedTags1.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"));

		final Stream<Tuple2<String, String>> scannedTags2 = Scannable.from(tagged2).tags();
		assertThat(scannedTags2.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"), Tuples.of( "2", "Two"));
	}

	@Test
	public void taggedHideFluxTest() {
		Flux<Integer> tagged1 =
				Flux.range(1, 10)
				    .hide()
				    .tag("1", "One");


		Flux<Integer> tagged2 = tagged1.filter(i -> i % 3 == 0)
		                               .tag("2", "Two")
		                               .hide();

		final Stream<Tuple2<String, String>> scannedTags1 = Scannable.from(tagged1).tags();
		assertThat(scannedTags1.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"));

		final Stream<Tuple2<String, String>> scannedTags2 = Scannable.from(tagged2).tags();
		assertThat(scannedTags2.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"), Tuples.of( "2", "Two"));
	}

	@Test
	public void taggedAppendedFluxTest() {
		Flux<Integer> tagged1 =
				Flux.range(1, 10)
				    .tag("1", "One")
				    .tag("2", "Two");

		final Stream<Tuple2<String, String>> scannedTags = Scannable.from(tagged1).tags();
		assertThat(scannedTags.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"), Tuples.of( "2", "Two"));
	}

	@Test
	public void taggedAppendedHideFluxTest() {
		Flux<Integer> tagged1 =
				Flux.range(1, 10)
				    .hide()
				    .tag("1", "One")
				    .tag("2", "Two");

		final Stream<Tuple2<String, String>> scannedTags = Scannable.from(tagged1).tags();
		assertThat(scannedTags.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"), Tuples.of( "2", "Two"));
	}

	@Test
	public void namedMonoTest() {
		Mono<Integer> named1 =
				Mono.just(1)
				    .name("100s");

		Mono<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .name("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}


	@Test
	public void namedHideMonoTest() {
		Mono<Integer> named1 =
				Mono.just(1)
				    .hide()
				    .name("100s");

		Mono<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .name("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}

	@Test
	public void namedOverridenMonoTest() {
		Mono<Integer> named1 =
				Mono.just(1)
				    .name("1s")
				    .name("100s");

		Mono<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .name("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}

	@Test
	public void namedOverridenHideMonoTest() {
		Mono<Integer> named1 =
				Mono.just(1)
				    .hide()
				    .name("1s")
				    .name("100s");

		Mono<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .name("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}

	@Test
	public void scannableNameMonoDefaultsToToString() {
		final Mono<Integer> flux = Mono.just(1)
		                               .map(i -> i + 10);

		assertThat(Scannable.from(flux).name())
				.isEqualTo(Scannable.from(flux).stepName())
				.isEqualTo("map");
	}

	@Test
	public void taggedMonoTest() {
		Mono<Integer> tagged1 =
				Mono.just(1)
				    .tag("1", "One");


		Mono<Integer> tagged2 = tagged1.filter(i -> i % 3 == 0)
		                               .tag("2", "Two")
		                               .hide();

		final Stream<Tuple2<String, String>> scannedTags1 = Scannable.from(tagged1).tags();
		assertThat(scannedTags1.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"));

		final Stream<Tuple2<String, String>> scannedTags2 = Scannable.from(tagged2).tags();
		assertThat(scannedTags2.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"), Tuples.of( "2", "Two"));
	}

	@Test
	public void taggedHideMonoTest() {
		Mono<Integer> tagged1 =
				Mono.just(1)
				    .hide()
				    .tag("1", "One");


		Mono<Integer> tagged2 = tagged1.filter(i -> i % 3 == 0)
		                               .tag("2", "Two")
		                               .hide();

		final Stream<Tuple2<String, String>> scannedTags1 = Scannable.from(tagged1).tags();
		assertThat(scannedTags1.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"));

		final Stream<Tuple2<String, String>> scannedTags2 = Scannable.from(tagged2).tags();
		assertThat(scannedTags2.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"), Tuples.of( "2", "Two"));
	}

	@Test
	public void taggedAppendedMonoTest() {
		Mono<Integer> tagged1 =
				Mono.just(1)
				    .tag("1", "One")
				    .tag("2", "Two");

		final Stream<Tuple2<String, String>> scannedTags = Scannable.from(tagged1).tags();
		assertThat(scannedTags.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"), Tuples.of( "2", "Two"));
	}

	@Test
	public void taggedAppendedHideMonoTest() {
		Mono<Integer> tagged1 = Mono
					.just(1)
				    .hide()
				    .tag("1", "One")
				    .tag("2", "Two");

		final Stream<Tuple2<String, String>> scannedTags = Scannable.from(tagged1).tags();
		assertThat(scannedTags.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"), Tuples.of( "2", "Two"));
	}

	@Test
	public void namedParallelFluxTest() {
		ParallelFlux<Integer> named1 =
				ParallelFlux.from(Mono.just(1))
				    .name("100s");

		ParallelFlux<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .name("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}

	@Test
	public void namedOverridenParallelFluxTest() {
		ParallelFlux<Integer> named1 =
				ParallelFlux.from(Mono.just(1))
				    .name("1s")
				    .name("100s");

		ParallelFlux<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .name("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}

	@Test
	public void scannableNameParallelFluxDefaultsToToString() {
		final ParallelFlux<Integer> flux = ParallelFlux.from(Mono.just(1))
		                               .map(i -> i + 10);

		assertThat(Scannable.from(flux).name())
				.isEqualTo(Scannable.from(flux).stepName())
				.isEqualTo("map");
	}

	@Test
	public void taggedParallelFluxTest() {
		ParallelFlux<Integer> tagged1 =
				ParallelFlux.from(Mono.just(1))
				    .tag("1", "One");


		ParallelFlux<Integer> tagged2 = tagged1.filter(i -> i % 3 == 0)
		                               .tag("2", "Two")
		                               .hide();

		Stream<Tuple2<String, String>> scannedTags1 = Scannable.from(tagged1).tags();
		assertThat(scannedTags1.iterator()).containsExactlyInAnyOrder(Tuples.of("1", "One"));

		Stream<Tuple2<String, String>> scannedTags2 = Scannable.from(tagged2).tags();
		assertThat(scannedTags2.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"), Tuples.of( "2", "Two"));
	}

	@Test
	public void taggedAppendedParallelFluxTest() {
		ParallelFlux<Integer> tagged1 =
				ParallelFlux.from(Mono.just(1))
				    .tag("1", "One")
				    .tag("2", "Two");

		final Stream<Tuple2<String, String>> scannedTags = Scannable.from(tagged1).tags();
		assertThat(scannedTags.iterator())
				.containsExactlyInAnyOrder(Tuples.of("1", "One"), Tuples.of( "2", "Two"));
	}

	@Test
	public void scanForParentIsSafe() {
		Scannable scannable = key -> "String";

		assertThat(scannable.scan(Scannable.Attr.PARENT))
				.isSameAs(Scannable.Attr.UNAVAILABLE_SCAN);
	}

	@Test
	public void scanForActualIsSafe() {
		Scannable scannable = key -> "String";

		assertThat(scannable.scan(Scannable.Attr.ACTUAL))
				.isSameAs(Scannable.Attr.UNAVAILABLE_SCAN);
	}

	@Test
	public void scanForRawParentOrActual() {
		Scannable scannable = key -> "String";

		assertThat(scannable.scanUnsafe(Scannable.Attr.ACTUAL))
				.isInstanceOf(String.class)
				.isEqualTo("String");

		assertThat(scannable.scan(Scannable.Attr.ACTUAL))
				.isSameAs(Scannable.Attr.UNAVAILABLE_SCAN);

		assertThat(scannable.scanUnsafe(Scannable.Attr.PARENT))
				.isInstanceOf(String.class)
				.isEqualTo("String");

		assertThat(scannable.scan(Scannable.Attr.PARENT))
				.isSameAs(Scannable.Attr.UNAVAILABLE_SCAN);
	}

	@Test
	public void attributeIsConversionSafe() {
		assertThat(Scannable.Attr.ACTUAL.isConversionSafe()).as("ACTUAL").isTrue();
		assertThat(Scannable.Attr.PARENT.isConversionSafe()).as("PARENT").isTrue();

		assertThat(Scannable.Attr.BUFFERED.isConversionSafe()).as("BUFFERED").isFalse();
		assertThat(Scannable.Attr.CAPACITY.isConversionSafe()).as("CAPACITY").isFalse();
		assertThat(Scannable.Attr.CANCELLED.isConversionSafe()).as("CANCELLED").isFalse();
		assertThat(Scannable.Attr.DELAY_ERROR.isConversionSafe()).as("DELAY_ERROR").isFalse();
		assertThat(Scannable.Attr.ERROR.isConversionSafe()).as("ERROR").isFalse();
		assertThat(Scannable.Attr.LARGE_BUFFERED.isConversionSafe()).as("LARGE_BUFFERED").isFalse();
		assertThat(Scannable.Attr.NAME.isConversionSafe()).as("NAME").isFalse();
		assertThat(Scannable.Attr.PREFETCH.isConversionSafe()).as("PREFETCH").isFalse();
		assertThat(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM.isConversionSafe()).as("REQUESTED_FROM_DOWNSTREAM").isFalse();
		assertThat(Scannable.Attr.TERMINATED.isConversionSafe()).as("TERMINATED").isFalse();
	}

	@Test
	public void operatorChainWithDebugMode() {
		Hooks.onOperatorDebug();

		List<String> downstream = new ArrayList<>();
		List<String> upstream = new ArrayList<>();

		final int line = Thread.currentThread().getStackTrace()[1].getLineNumber();
		Mono<?> m=
				Flux.from(s -> {
					Scannable thisSubscriber = Scannable.from(s);
					assertThat(thisSubscriber.isScanAvailable()).as("thisSubscriber.isScanAvailable").isTrue();
					thisSubscriber.steps().forEach(downstream::add);
				})
				    .map(a -> a)
				    .delayElements(Duration.ofMillis(10))
				    .filter(a -> true)
				    .reduce((a, b) -> b);

		m.subscribe();

		Scannable thisOperator = Scannable.from(m);
		assertThat(thisOperator.isScanAvailable()).as("thisOperator.isScanAvailable").isTrue();

		thisOperator.steps().forEach(upstream::add);

		assertThat(downstream).containsExactly(
				"Flux.from ⇢ at reactor.core.ScannableTest.operatorChainWithDebugMode(ScannableTest.java:"+(line+2)+")",
				"Flux.map ⇢ at reactor.core.ScannableTest.operatorChainWithDebugMode(ScannableTest.java:"+(line+7)+")",
				"Flux.delayElements ⇢ at reactor.core.ScannableTest.operatorChainWithDebugMode(ScannableTest.java:"+(line+8)+")",
				"Flux.filter ⇢ at reactor.core.ScannableTest.operatorChainWithDebugMode(ScannableTest.java:"+(line+9)+")",
				"Flux.reduce ⇢ at reactor.core.ScannableTest.operatorChainWithDebugMode(ScannableTest.java:"+(line+10)+")",
				"lambda");

		assertThat(upstream).containsExactly(
				"Flux.from ⇢ at reactor.core.ScannableTest.operatorChainWithDebugMode(ScannableTest.java:"+(line+2)+")",
				"Flux.map ⇢ at reactor.core.ScannableTest.operatorChainWithDebugMode(ScannableTest.java:"+(line+7)+")",
				"Flux.delayElements ⇢ at reactor.core.ScannableTest.operatorChainWithDebugMode(ScannableTest.java:"+(line+8)+")",
				"Flux.filter ⇢ at reactor.core.ScannableTest.operatorChainWithDebugMode(ScannableTest.java:"+(line+9)+")",
				"Flux.reduce ⇢ at reactor.core.ScannableTest.operatorChainWithDebugMode(ScannableTest.java:"+(line+10)+")");
	}

	@Test
	public void operatorChainWithLastSubscriber() {
		AtomicReference<Subscription> subRef = new AtomicReference<>(null);

		Mono<String> m=
				Flux.just("foo")
				    .map(a -> a)
				    .filter(a -> true)
				    .reduce((a, b) -> b)
				    .doOnSubscribe(subRef::set);

		m.subscribe();

		Scannable lastSubscriber = Scannable.from(subRef.get());
		assertThat(lastSubscriber.isScanAvailable()).as("lastSubscriber.isScanAvailable").isTrue();

		Stream<String> chain = lastSubscriber.steps();

		assertThat(chain).containsExactly("just", "map", "filter", "reduce", "peek", "lambda");
	}

	@Test
	public void operatorChainWithLastOperator() {
		Mono<String> m =
				Flux.concat(Mono.just("foo"), Mono.just("bar"))
				    .map(a -> a)
				    .filter(a -> true)
				    .reduce((a, b) -> b)
				    .doOnSubscribe(sub -> {});

		Scannable operator = Scannable.from(m);
		assertThat(operator.isScanAvailable()).as("operator.isScanAvailable").isTrue();

		assertThat(operator.steps())
				.containsExactly("source(FluxConcatArray)", "map", "filter", "reduce", "peek");
	}

	@Test
	public void operatorChainWithCheckpoint() {
		Flux<String> flux  = Flux.just("foo")
		                         .checkpoint("checkpointHere", true)
		                         .map(a -> a);

		assertThat(Scannable.from(flux).steps())
				.containsExactly(
						"source(FluxJust)",
						"Flux.checkpoint ⇢ at reactor.core.ScannableTest.operatorChainWithCheckpoint(ScannableTest.java:612)",
						"map"
				);
	}

	@Test
	public void operatorChainWithLightCheckpoint() {
		Flux<String> flux  = Flux.just("foo")
		                         .checkpoint("checkpointHere")
		                         .map(a -> a);

		assertThat(Scannable.from(flux).steps())
				.containsExactly(
						"source(FluxJust)",
						"checkpoint(\"checkpointHere\")",
						"map"
				);
	}

	@Test
	public void operatorChainWithoutDebugMode() {
		List<String> downstream = new ArrayList<>();

		Mono<?> m=
				Flux.from(s -> {
					Scannable thisSubscriber = Scannable.from(s);
					assertThat(thisSubscriber.isScanAvailable()).as("thisSubscriber.isScanAvailable").isTrue();
					thisSubscriber.steps().forEach(downstream::add);
				})
				    .map(a -> a)
				    .filter(a -> true)
				    .reduce((a, b) -> b);

		m.subscribe();

		assertThat(downstream)
				.as("from downstream")
				.containsExactly(
				"map",
				"filter",
				"reduce",
				"lambda");


		Scannable thisOperator = Scannable.from(m);

		assertThat(thisOperator.isScanAvailable())
				.as("thisOperator.isScanAvailable")
				.isTrue();
		assertThat(thisOperator.steps())
				.as("from upstream")
				.containsExactly(
				"source(FluxSource)",
				"map",
				"filter",
				"reduce");
	}

	@Test
	void stepNameSmokeTest() {
		Flux<String> f = Flux.just("foo")
		                     .filter(i -> true)
		                     .map(Function.identity());
		assertThat(Scannable.from(f).steps())
				.as("publisher chain")
				.containsExactly("source(FluxJust)", "filter", "map");

		AtomicReference<Subscription> subRef = new AtomicReference<>();
		f.doOnSubscribe(subRef::set)
		 .blockLast();
		assertThat(Scannable.from(subRef.get()).steps())
				.as("subscriber chain")
				.containsExactly("just", "filter", "map", "peek", "blockLast");
	}

	@Test
	void stepNameSmokeTestWithDebugMode() {
		Hooks.onOperatorDebug();

		final int line = Thread.currentThread().getStackTrace()[1].getLineNumber();
		Flux<String> f = Flux.just("foo")
		                     .filter(i -> true)
		                     .map(Function.identity());
		assertThat(Scannable.from(f).steps())
				.as("publisher chain")
				.containsExactly(
						"Flux.just ⇢ at reactor.core.ScannableTest.stepNameSmokeTestWithDebugMode(ScannableTest.java:"+ (line+1) + ")",
						"Flux.filter ⇢ at reactor.core.ScannableTest.stepNameSmokeTestWithDebugMode(ScannableTest.java:"+ (line+2) + ")",
						"Flux.map ⇢ at reactor.core.ScannableTest.stepNameSmokeTestWithDebugMode(ScannableTest.java:"+ (line+3) + ")"
				);

		AtomicReference<Subscription> subRef = new AtomicReference<>();
		f.doOnSubscribe(subRef::set)
		 .blockLast();
		assertThat(Scannable.from(subRef.get()).steps())
				.as("subscriber chain")
				.containsExactly(
						"Flux.just ⇢ at reactor.core.ScannableTest.stepNameSmokeTestWithDebugMode(ScannableTest.java:"+ (line+1) + ")",
						"Flux.filter ⇢ at reactor.core.ScannableTest.stepNameSmokeTestWithDebugMode(ScannableTest.java:"+ (line+2) + ")",
						"Flux.map ⇢ at reactor.core.ScannableTest.stepNameSmokeTestWithDebugMode(ScannableTest.java:"+ (line+3) + ")",
						"Flux.doOnSubscribe ⇢ at reactor.core.ScannableTest.stepNameSmokeTestWithDebugMode(ScannableTest.java:"+ (line+13) + ")",
						"blockLast"
				);
	}

}
