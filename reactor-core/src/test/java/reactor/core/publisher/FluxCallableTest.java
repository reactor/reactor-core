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

import java.io.IOException;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxCallableTest {

	@Test
	public void callableReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>fromCallable(() -> null).log().flux()
		                                      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.fromCallable(() -> 1)
		    .flux()
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.fromCallable(() -> 1)
		    .flux()
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(1);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void callableThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.fromCallable(() -> {
			throw new IOException("forced failure");
		})
		    .flux()
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(IOException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void scanOperator(){
	    FluxCallable<Integer> test = new FluxCallable<>(() -> 1	);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
