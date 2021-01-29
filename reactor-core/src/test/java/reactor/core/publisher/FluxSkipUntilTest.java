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
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;

public class FluxSkipUntilTest extends FluxOperatorTest<String, String> {

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(scenario(f -> f.skipUntil(d -> {
					throw exception();
				})
				)
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.skipUntil(item(1)::equals))
				.receiveValues(item(1), item(2))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(scenario(f -> f.skipUntil(item(1)::equals)));
	}

	@Test
	public void normalHidden() {
		StepVerifier.create(Flux.range(1, 10)
		                        .hide()
		                        .skipUntil(v -> v > 4))
		            .expectNext(5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	public void normal() {
		StepVerifier.create(Flux.range(1, 10)
		                        .skipUntil(v -> v > 4))
		            .expectNext(5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	// see https://github.com/reactor/reactor-core/issues/2578
	@Test
	@Timeout(5L)
	public void conditionalOptimization() {
		StepVerifier.create(
				Flux.range(1, 5)
						.skipUntil(v -> v > 1)
						.flatMap(v -> Mono.just(v), 1) // to request just 1 item
		)
				.expectNext(2, 3, 4, 5)
				.verifyComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxSkipUntil<Integer> test = new FluxSkipUntil<>(parent, p -> true);

		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSkipUntil.SkipUntilSubscriber<Integer> test = new FluxSkipUntil.SkipUntilSubscriber<>(actual, i -> true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }
}
