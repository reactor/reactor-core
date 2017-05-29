/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.time.Duration;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

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
    public void scanSubscriber() {
        Subscriber<Tuple2<Long, String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxElapsed.ElapsedSubscriber<String> test = new FluxElapsed.ElapsedSubscriber<>(actual, Schedulers.single());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
    }
}