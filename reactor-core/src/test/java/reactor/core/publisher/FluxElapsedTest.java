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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Scannable.from;

public class FluxElapsedTest {

	Flux<Tuple2<Long, String>> scenario_aFluxCanBeBenchmarked(){
		return Flux.just("test")
		           .elapsed();
	}

	@Test
	public void aFluxCanBeBenchmarked(){
		StepVerifier.withVirtualTime(this::scenario_aFluxCanBeBenchmarked,0)
		            .thenAwait(Duration.ofSeconds(2))
		            .thenRequest(1)
		            .expectNextMatches(t -> t.getT1() == 2000 && t.getT2().equals("test"))
		            .verifyComplete();
	}

	@Test
    public void scanOperator() {
		Flux<Tuple2<Long, Integer>> test = Flux.just(1).elapsed(Schedulers.single());

		assertThat(test).isInstanceOf(Scannable.class);
		assertThat(from(test).scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.single());
		assertThat(from(test).scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Tuple2<Long, String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxElapsed.ElapsedSubscriber<String> test = new FluxElapsed.ElapsedSubscriber<>(actual, Schedulers.single());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.single());
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }
}
