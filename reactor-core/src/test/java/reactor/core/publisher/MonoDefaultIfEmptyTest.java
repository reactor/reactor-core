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
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoDefaultIfEmptyTest {

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoDefaultIfEmpty<>(null, 1);
		});
	}

	@Test
	public void valueNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Mono.never().defaultIfEmpty(null);
		});
	}

	@Test
	public void error() {
		StepVerifier.create(Mono.error(new RuntimeException("forced failure"))
		                        .defaultIfEmpty("blah"))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void errorHide() {
		StepVerifier.create(Mono.error(new RuntimeException("forced failure"))
		                        .hide()
		                        .defaultIfEmpty("blah"))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void nonEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1).defaultIfEmpty(10).subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

	}
	@Test
	public void nonEmptyHide() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1).hide().defaultIfEmpty(10).subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void nonEmptyHideBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.just(1).hide().defaultIfEmpty(10).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void nonEmptyBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.just(1).defaultIfEmpty(10).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>empty().defaultIfEmpty(10).subscribe(ts);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void emptyHide() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>empty().hide().defaultIfEmpty(10).subscribe(ts);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void emptyBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.<Integer>empty().defaultIfEmpty(10).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void emptyBackpressuredHide() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.<Integer>empty().hide().defaultIfEmpty(10).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void scanOperator() {
		MonoDefaultIfEmpty<Integer> test = new MonoDefaultIfEmpty<>(Mono.empty(), 10);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}
