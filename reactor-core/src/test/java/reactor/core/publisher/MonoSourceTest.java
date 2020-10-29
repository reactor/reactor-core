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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSourceTest {

	@Test
	public void empty() {
		Mono<Integer> m = Mono.from(Flux.empty());
		assertThat(m == Mono.<Integer>empty()).isTrue();
		StepVerifier.create(m)
		            .verifyComplete();
	}

	@Test
	public void just() {
		Mono<Integer> m = Mono.from(Flux.just(1));
		assertThat(m).isInstanceOf(MonoJust.class);
		StepVerifier.create(m)
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void error() {
		Mono<Integer> m = Mono.from(Flux.error(new Exception("test")));
		assertThat(m).isInstanceOf(MonoError.class);
		StepVerifier.create(m)
		            .verifyErrorMessage("test");
	}

	@Test
	public void errorPropagate() {
		Mono<Integer> m = Mono.from(Flux.error(new Error("test")));
		assertThat(m).isInstanceOf(MonoError.class);
		StepVerifier.create(m)
		            .verifyErrorMessage("test");
	}

	@Test
	public void justNext() {
		StepVerifier.create(Mono.from(Flux.just(1, 2, 3)))
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void asJustNext() {
		StepVerifier.create(Flux.just(1, 2, 3).as(Mono::from))
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void monoNext() {
		StepVerifier.create(Flux.just(1, 2, 3).next())
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void monoDirect() {
		StepVerifier.create(Flux.just(1).as(Mono::fromDirect))
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void monoDirectHidden() {
		StepVerifier.create(Flux.just(1).hide().as(Mono::fromDirect))
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void monoDirectIdentity() {
		StepVerifier.create(Mono.just(1).as(Mono::fromDirect))
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void monoDirectPlainFuseable() {
		StepVerifier.create(Mono.just(1).as(TestPubFuseable::new))
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void monoDirectPlain() {
		StepVerifier.create(Mono.just(1).as(TestPub::new))
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void monoFromFluxThatIsItselfFromMono() {
		AtomicBoolean emitted = new AtomicBoolean();
		AtomicBoolean terminated = new AtomicBoolean();
		AtomicBoolean cancelled = new AtomicBoolean();
		AtomicBoolean succeeded = new AtomicBoolean();

		Mono<String> withCallback = Mono.just("foo")
		                                .doOnNext(v -> emitted.set(true));

		Mono<String> original = withCallback
				.doOnCancel(() -> cancelled.set(true))
				.doOnSuccess(v -> succeeded.set(true))
				.doOnTerminate(() -> terminated.set(true))
				.hide();

		assertThat(withCallback).as("withCallback is not Callable")
		                    .isNotInstanceOf(Fuseable.ScalarCallable.class)
		                    .isNotInstanceOf(Callable.class);

		assertThat(original).as("original is not callable Mono")
		                  .isNotInstanceOf(Fuseable.class)
		                  .isNotInstanceOf(Fuseable.ScalarCallable.class)
		                  .isNotInstanceOf(Callable.class);

		Flux<String> firstConversion = Flux.from(original);
		Mono<String> secondConversion = Mono.from(firstConversion);

		assertThat(secondConversion.block()).isEqualTo("foo");

		assertThat(emitted).as("emitted").isTrue();
		assertThat(succeeded).as("succeeded").isTrue();
		assertThat(cancelled).as("cancelled").isFalse();
		assertThat(terminated).as("terminated").isTrue();

		assertThat(secondConversion).as("conversions negated").isSameAs(original);
	}

	@Test
	public void monoFromFluxThatIsItselfFromMonoFuseable() {
		Mono<String> original = Mono.just("foo").map(v -> v + "bar");

		Flux<String> firstConversion = Flux.from(original);
		Mono<String> secondConversion = Mono.from(firstConversion);

		assertThat(original).isInstanceOf(Fuseable.class);
		assertThat(secondConversion).isInstanceOf(Fuseable.class);
		assertThat(secondConversion.block()).isEqualTo("foobar");
		assertThat(secondConversion).as("conversions negated").isSameAs(original);
	}

	@Test
	public void monoFromFluxThatIsItselfFromMono_scalarCallableNotOptimized() {
		Mono<String> original = Mono.just("foo");

		Flux<String> firstConversion = Flux.from(original);
		Mono<String> secondConversion = Mono.from(firstConversion);

		assertThat(secondConversion.block()).isEqualTo("foo");
		assertThat(secondConversion).as("conversions not negated but equivalent")
		                            .isNotSameAs(original)
		                            .hasSameClassAs(original);
	}

	@Test
	public void monoFromFluxItselfMonoToFlux() {
		Mono<String> original = Mono.just("foo").hide();

		Flux<String> firstConversion = original.flux();
		Mono<String> secondConversion = Mono.from(firstConversion);

		assertThat(secondConversion.block()).isEqualTo("foo");
		assertThat(secondConversion).as("conversions negated").isSameAs(original);
	}

	@Test
	public void monoFromFluxItselfMonoToFlux_fuseable() {
		Mono<String> original = Mono.just("foo").map(v -> v + "bar");

		Flux<String> firstConversion = original.flux();
		Mono<String> secondConversion = Mono.from(firstConversion);

		assertThat(original).isInstanceOf(Fuseable.class);
		assertThat(secondConversion).isInstanceOf(Fuseable.class);
		assertThat(secondConversion.block()).isEqualTo("foobar");
		assertThat(secondConversion).as("conversions negated").isSameAs(original);
	}

	@Test
	public void monoFromFluxItselfMonoToFlux_scalarCallableNotOptimized() {
		Mono<String> original = Mono.just("foo");

		Flux<String> firstConversion = original.flux();
		Mono<String> secondConversion = Mono.from(firstConversion);

		assertThat(secondConversion.block()).isEqualTo("foo");
		assertThat(secondConversion).as("conversions not negated but equivalent")
		                            .isNotSameAs(original)
		                            .hasSameClassAs(original);
	}

	final static class TestPubFuseable implements Publisher<Integer>, Fuseable {
		final Mono<Integer> m;

		TestPubFuseable(Mono<Integer> mono) {
			this.m = mono;
		}

		@Override
		public void subscribe(Subscriber<? super Integer> s) {
			m.subscribe(s);
		}
	}

	final static class TestPub implements Publisher<Integer>, Fuseable {
		final Mono<Integer> m;

		TestPub(Mono<Integer> mono) {
			this.m = mono;
		}

		@Override
		public void subscribe(Subscriber<? super Integer> s) {
			m.subscribe(s);
		}
	}

	@Test
	public void transform() {
		StepVerifier.create(Mono.just(1).transform(m -> Flux.just(1, 2, 3)))
	                .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void onAssemblyDescription() {
		String monoOnAssemblyStr = Mono.just(1).checkpoint("onAssemblyDescription").toString();
		assertThat(monoOnAssemblyStr).as("Description not included").contains("checkpoint(\"onAssemblyDescription\")");
	}

	@Test
	public void scanSubscriber() {
		Flux<String> source = Flux.just("foo").map(i -> i);
		MonoSource<String> test = new MonoSource<>(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableSubscriber(){
		Mono<String> source = Mono.just("foo");
		MonoSourceFuseable<String> test = new MonoSourceFuseable<>(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriberHide() {
		Flux<String> source = Flux.just("foo").hide();
		MonoSource<String> test = new MonoSource<>(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriberIgnore() {
		Flux<String> source = Flux.just("foo").map(i -> i);
		MonoIgnorePublisher<String> test = new MonoIgnorePublisher<>(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriberFrom() {
		Flux<String> source = Flux.just("foo").map(i -> i);
		MonoFromPublisher<String> test = new MonoFromPublisher<>(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
	}
}
