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

import java.io.IOException;
import java.time.Duration;

import org.junit.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class MonoSubscribeOnCallableTest {

	@Test
	public void callableReturnsNull() {
		StepVerifier.create(new MonoSubscribeOnCallable<>(() -> null, Schedulers.single()))
		            .expectError(NullPointerException.class)
		            .verify();
	}

	@Test
	public void normal() {
		StepVerifier.create(new MonoSubscribeOnCallable<>(() -> 1, Schedulers.single()))
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void normalBackpressured() {
		StepVerifier.withVirtualTime( () -> new MonoSubscribeOnCallable<>(() -> 1,
				Schedulers.single()), 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenRequest(1)
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void callableThrows() {
		StepVerifier.create(new MonoSubscribeOnCallable<>(() -> {
			throw new IOException("forced failure");
		}, Schedulers.single()))
		            .expectErrorMatches(e -> e instanceof IOException
				            && e.getMessage().equals("forced failure"))
		            .verify();
	}
}