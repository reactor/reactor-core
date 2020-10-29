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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import org.junit.jupiter.api.Test;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Scannable.from;

public class FluxSubscribeOnValueTest {

	ConcurrentMap<Integer, Integer> execs = new ConcurrentHashMap<>();

	@Test
	public void finishedConstantsAreNotSame() {
		assertThat(FluxSubscribeOnValue.ScheduledScalar.FINISHED)
				.isNotSameAs(FluxSubscribeOnValue.ScheduledEmpty.FINISHED);
	}

	@Test
	public void testSubscribeOnValueFusion() {

		StepVerifier.create(Flux.range(1, 100)
		                        .flatMap(f -> Flux.just(f)
		                                          .subscribeOn(Schedulers.parallel())
		                                          .log("testSubscribeOnValueFusion", Level.FINE)
		                                          .map(this::slow)))
		            .expectFusion(Fuseable.ASYNC, Fuseable.NONE)
		            .expectNextCount(100)
		            .verifyComplete();

		int minExec = 2;

		for (Integer counted : execs.values()) {
			assertThat(counted).as("Thread used less than %d times", minExec).isGreaterThanOrEqualTo(minExec);
		}

	}

	int slow(int slow){
		try {
			execs.computeIfAbsent(Thread.currentThread()
			                            .hashCode(), i -> 0);
			execs.compute(Thread.currentThread()
			                    .hashCode(), (k, v) -> v + 1);
			Thread.sleep(10);
			return slow;
		}
		catch (InterruptedException e) {
			throw Exceptions.bubble(e);
		}
	}

	@Test
    public void scanOperator() {
		final Flux<Integer> test = Flux.just(1).subscribeOn(Schedulers.immediate());

		assertThat(test).isInstanceOf(Scannable.class)
		                .isInstanceOf(FluxSubscribeOnValue.class);

		assertThat(from(test).scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(from(test).scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSubscribeOnValue.ScheduledScalar<Integer> test =
        		new FluxSubscribeOnValue.ScheduledScalar<Integer>(actual, 1, Schedulers.single());

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.future = FluxSubscribeOnValue.ScheduledScalar.FINISHED;
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.future = OperatorDisposables.DISPOSED;
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();

        assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.single());
    }
}
