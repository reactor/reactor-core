/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

public class FluxMapSignalTest extends FluxOperatorTest<String, String> {

	@Test(expected = IllegalArgumentException.class)
	public void allNull(){
		Flux.never().flatMap(null, null, null);
	}

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.shouldAssertPostTerminateState(false);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.flatMap(Flux::just, Flux::error, Flux::empty)),

				scenario(f -> f.flatMap(Flux::just, Flux::error, null)),

				scenario(f -> f.flatMap(Flux::just, null, null))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.flatMap(d -> null, null, null))
				,

				scenario(f -> f.flatMap(null, null, () -> null))

		);
	}

	@Test
    public void completeOnlyBackpressured() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create(0L);
        
        new FluxMapSignal<>(Flux.empty(), null, null, () -> 1)
        .subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void errorOnlyBackpressured() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create(0L);
        
        new FluxMapSignal<>(Flux.error(new RuntimeException()), null, e -> 1, null)
        .subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(1)
        .assertNoError()
        .assertComplete();
    }

	@Test
	public void flatMapSignal() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMap(d -> Flux.just(d * 2),
				                        e -> Flux.just(99),
				                        () -> Flux.just(10)))
		            .expectNext(2, 4, 6, 10)
		            .verifyComplete();
	}

	@Test
	public void flatMapSignalError() {
		StepVerifier.create(Flux.just(1, 2, 3).concatWith(Flux.error(new Exception("test")))
		                        .flatMap(d -> Flux.just(d * 2),
				                        e -> Flux.just(99),
				                        () -> Flux.just(10)))
		            .expectNext(2, 4, 6, 99)
		            .verifyComplete();
	}

	@Test
	public void flatMapSignal2() {
		StepVerifier.create(Mono.just(1)
		                        .flatMapMany(d -> Flux.just(d * 2),
				                        e -> Flux.just(99),
				                        () -> Flux.just(10)))
		            .expectNext(2, 10)
		            .verifyComplete();
	}
}
