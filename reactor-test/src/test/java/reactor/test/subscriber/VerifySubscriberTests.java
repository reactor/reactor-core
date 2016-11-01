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
package reactor.test.subscriber;

import java.time.Duration;

import org.junit.Test;
import reactor.core.publisher.Flux;

/**
 * @author Stephane Maldini
 */
public class VerifySubscriberTests {


	@Test(expected = IllegalStateException.class)
	public void notSubscribed() {
		VerifySubscriber.create()
		                .expectNext("foo")
		                .expectComplete()
		                .verify(Duration.ofMillis(100));
	}



	@Test(expected = AssertionError.class)
	public void subscribedTwice() {
		Flux<String> flux = Flux.just("foo", "bar");

		VerifySubscriber<String> s =
				VerifySubscriber.<String>create().expectNext("foo", "bar")
				                                 .expectComplete();

		flux.subscribe(s);
		flux.subscribe(s);
		s.verify();
	}
}
