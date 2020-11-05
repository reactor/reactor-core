/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.junit.jupiter.api.Test;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable.Attr;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;

/**
 * @author Simon Baslé
 */
class MonoTimedTest {

	@Test
	void schedulerIsPassedToFluxTimedSubscriber() {
		final MonoTimed<String> operator = new MonoTimed<>(Mono.just("example"), Schedulers.boundedElastic());

		final CoreSubscriber<? super String> subscriber = operator.subscribeOrReturn(Operators.emptySubscriber());

		assertThat(subscriber).isInstanceOfSatisfying(FluxTimed.TimedSubscriber.class, ts ->
				assertThat(ts.clock).as("clock").isSameAs(Schedulers.boundedElastic()));
	}

	@Test
	void scanOperator() {
		Mono<String> source = Mono.just("example");
		MonoTimed<String> operator = new MonoTimed<>(source, Schedulers.immediate());

		assertThat(operator.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(SYNC);
		assertThat(operator.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(0);
		assertThat(operator.scan(Attr.PARENT)).as("PARENT").isSameAs(source);
	}

}