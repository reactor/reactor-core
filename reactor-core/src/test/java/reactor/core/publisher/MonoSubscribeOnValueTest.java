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

import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSubscribeOnValueTest {

	@Test
	public void testSubscribeOnValueFusion() {

		StepVerifier.create(Mono.just(1)
		                        .flatMapMany(f -> Mono.just(f + 1)
		                                              .subscribeOn(Schedulers.parallel())
		                                              .map(this::slow)))
		            .expectFusion(Fuseable.ASYNC, Fuseable.NONE)
		            .expectNext(2)
		            .verifyComplete();
	}

	int slow(int slow){
		try {
			Thread.sleep(10);
			return slow;
		}
		catch (InterruptedException e) {
			throw Exceptions.bubble(e);
		}
	}

	@Test
	public void scanOperator() {
		MonoSubscribeOnValue<String> test = new MonoSubscribeOnValue<>("foo", Schedulers.immediate());

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}
}
