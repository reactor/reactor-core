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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxOnBackpressureDropTest {

	@Test
	public void source1Null() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxOnBackpressureDrop<>(null);
		});
	}

	@Test
	public void source2Null() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxOnBackpressureDrop<>(null, v -> {
			});
		});
	}

	@Test
	public void onDropNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never().onBackpressureDrop(null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .onBackpressureDrop()
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .onBackpressureError()
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .onBackpressureDrop()
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void someDrops() {
		Sinks.Many<Integer> tp = Sinks.unsafe().many().multicast().directBestEffort();

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		List<Integer> drops = new ArrayList<>();

		tp.asFlux()
		  .onBackpressureDrop(drops::add)
		  .subscribe(ts);

		tp.emitNext(1, FAIL_FAST);

		ts.request(2);

		tp.emitNext(2, FAIL_FAST);
		tp.emitNext(3, FAIL_FAST);
		tp.emitNext(4, FAIL_FAST);

		ts.request(1);

		tp.emitNext(5, FAIL_FAST);
		tp.emitComplete(FAIL_FAST);

		ts.assertValues(2, 3, 5)
		  .assertComplete()
		  .assertNoError();

		assertThat(drops).containsExactly(1, 4);
	}

	@Test
	public void onDropThrows() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .onBackpressureDrop(e -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void onBackpressureDrop() {
		StepVerifier.create(Flux.range(1, 100)
		                        .onBackpressureDrop(), 0)
		            .thenRequest(5)
		            .expectNext(1, 2, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void scanOperator(){
	    FluxOnBackpressureDrop<Integer> test = new FluxOnBackpressureDrop<>(Flux.just(1));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxOnBackpressureDrop.DropSubscriber<Integer> test =
        		new FluxOnBackpressureDrop.DropSubscriber<>(actual, t -> {});
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

        test.onComplete();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }
}
