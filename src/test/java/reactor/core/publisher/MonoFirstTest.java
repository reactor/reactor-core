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

import org.junit.Assert;
import org.junit.Test;
import reactor.test.subscriber.AssertSubscriber;

public class MonoFirstTest {

	@Test(timeout = 5000)
	public void allEmpty() {
		Assert.assertNull(Mono.first(Mono.empty(),
				Mono.delay(Duration.ofMillis(250))
				    .ignoreElement())
		                      .block());
	}

	@Test(timeout = 5000)
	public void someEmpty() {
		Assert.assertNull(Mono.first(Mono.empty(), Mono.delay(Duration.ofMillis(250)))
		                      .block());
	}

	@Test//(timeout = 5000)
	public void all2NonEmpty() {
		Assert.assertEquals(Integer.MIN_VALUE,
				Mono.first(Mono.delayMillis(150)
				               .map(i -> Integer.MIN_VALUE), Mono.delayMillis(250))
				    .block());
	}

	@Test
	public void pairWise() {
		Mono<Integer> f = Mono.first(Mono.just(1), Mono.just(2))
		                      .or(Mono.just(3));
		f.subscribeWith(AssertSubscriber.create())
		 .assertValues(1)
		 .assertComplete();
	}
}
