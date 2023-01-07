/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

public class MonoSingleOptionalTest {

    @Nested
    class ConcreteClassConsistency {
        // tests Mono.singleOptional returned classes

        @Test
        void monoWithScalarEmpty() {
            Mono<Integer> source = Mono.empty();
            Mono<Optional<Integer>> singleOptional = source.singleOptional();

            assertThat(source).as("source").isInstanceOf(Fuseable.ScalarCallable.class);
            assertThat(singleOptional).as("singleOptional")
                              .isInstanceOf(MonoJust.class)
                              .isInstanceOf(Fuseable.ScalarCallable.class);
        }

        @Test
        void monoWithScalarError() {
            Mono<Integer> source = Mono.error(new IllegalStateException("test"));
            Mono<Optional<Integer>> singleOptional = source.singleOptional();

            assertThat(source).as("source").isInstanceOf(Fuseable.ScalarCallable.class);
            assertThat(singleOptional).as("singleOptional")
                              .isInstanceOf(MonoError.class)
                              .isInstanceOf(Fuseable.ScalarCallable.class);
        }

        @Test
        void monoWithScalarValue() {
            Mono<Integer> source = Mono.just(1);
            Mono<Optional<Integer>> single = source.singleOptional();

            assertThat(source).as("source").isInstanceOf(Fuseable.ScalarCallable.class);
            assertThat(single).as("singleOptional")
                              .isInstanceOf(MonoJust.class)
                              .isInstanceOf(Fuseable.ScalarCallable.class);
        }

        @Test
        void monoWithCallable() {
            Mono<Integer> source = Mono.fromSupplier(() -> 1);
            Mono<Optional<Integer>> single = source.singleOptional();

            assertThat(source).as("source")
                              .isInstanceOf(Callable.class)
                              .isNotInstanceOf(Fuseable.ScalarCallable.class);
            assertThat(single).as("singleOptional").isInstanceOf(MonoSingleOptionalCallable.class);
        }

        @Test
        void monoWithNormal() {
            Mono<Integer> source = Mono.just(1).hide();
            Mono<Optional<Integer>> single = source.singleOptional();

            assertThat(source).as("source").isNotInstanceOf(Callable.class); //excludes ScalarCallable too
            assertThat(single).as("singleOptional").isInstanceOf(MonoSingleOptional.class);
        }
    }

    @Test
    void source1Null() {
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
            new MonoSingleOptional<>(null);
        });
    }
    
    @Test
	public void callableEmpty() {
		StepVerifier.create(Mono.empty().singleOptional())
					.expectNext(Optional.empty())
					.verifyComplete();
	}

	@Test
	public void callableValued() {
		StepVerifier.create(Mono.just("foo").singleOptional())
		            .expectNext(Optional.of("foo"))
		            .verifyComplete();
	}

	@Test
	public void normalEmpty() {
		StepVerifier.create(Mono.empty().hide().singleOptional())
							.expectNext(Optional.empty())
							.verifyComplete();
	}

	@Test
	public void normalValued() {
		StepVerifier.create(Mono.just("foo").hide().singleOptional())
		            .expectNext(Optional.of("foo"))
		            .verifyComplete();
	}

	@Test
	void fusionMonoSingleFusion() {
		Mono<Optional<Integer>> fusedCase = Mono
				.just(1)
				.map(Function.identity())
				.singleOptional();

		assertThat(fusedCase)
				.as("fusedCase assembly check")
				.isInstanceOf(MonoSingleOptional.class)
				.isNotInstanceOf(Fuseable.class);

		assertThatCode(() -> fusedCase.filter(v -> true).block())
				.as("fusedCase fused")
				.doesNotThrowAnyException();
	}

	@Test
	public void scanOperator(){
	    MonoSingleOptional<String> test = new MonoSingleOptional<>(Mono.just("foo"));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

    @Test
    public void scanSubscriber() {
        CoreSubscriber<Optional<String>>
                actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
        MonoSingleOptional.SingleOptionalSubscriber<String> test = new MonoSingleOptional.SingleOptionalSubscriber<>(
                actual);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
