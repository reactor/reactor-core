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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoAllTest {

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoAll<>(null, v -> true);
		});
	}

	@Test
	public void predicateNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoAll<>(null, null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create();

		Flux.range(1, 10).all(v -> true).subscribe(ts);

		ts.assertValues(true)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).all(v -> true).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(1);

		ts.assertValues(true)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void someMatch() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create();

		Flux.range(1, 10).all(v -> v < 6).subscribe(ts);

		ts.assertValues(false)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void someMatchBackpressured() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).all(v -> v < 6).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(1);

		ts.assertValues(false)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void predicateThrows() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create();

		Flux.range(1, 10).all(v -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> assertThat(e).hasMessageContaining("forced failure"));
	}

	@Test
	public void scanOperator(){
	    MonoAll<Integer> test = new MonoAll<>(Flux.just(1, 2, 3), v -> true);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<Boolean> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoAll.AllSubscriber<String> test = new MonoAll.AllSubscriber<>(actual, String::isEmpty);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

}
