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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.publisher.MonoOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;

public class MonoMapTest extends MonoOperatorTest<String, String> {

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.map(a -> a))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.map(a ->  null))
		);
	}

	final Mono<Integer> just = Mono.just(1);

	@Test
	public void nullSource() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoMap<Integer, Integer>(null, v -> v);
		});
	}

	@Test
	public void nullMapper() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			just.map(null);
		});
	}

	@Test
	public void simpleMapping() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		just.map(v -> v + 1)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertValues(2)
		  .assertComplete();
	}

	@Test
	public void simpleMappingBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		just.map(v -> v + 1)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(1);

		ts.assertNoError()
		  .assertValues(2)
		  .assertComplete();
	}

	@Test
	public void mapperThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		just.map(v -> {
			throw new RuntimeException("forced failure");
		})
		    .subscribe(ts);

		ts.assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNoValues()
		  .assertNotComplete();
	}

	@Test
	public void mapperReturnsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		just.map(v -> null)
		    .subscribe(ts);

		ts.assertError(NullPointerException.class)
		  .assertNoValues()
		  .assertNotComplete();
	}

	@Test
	public void mapFilter() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		just
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mapFilterBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		just
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void hiddenMapFilter() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		just
		    .hide()
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void hiddenMapFilterBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		just
		    .hide()
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void hiddenMapHiddenFilterBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		just
		    .hide()
		    .map(v -> v + 1)
		    .hide()
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.request(1);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void scanOperator(){
		MonoMap<String, String> test = new MonoMap<>(Mono.just("foo"), s -> s.toUpperCase());

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperator(){
		MonoMapFuseable<String, String> test = new MonoMapFuseable<>(Mono.just("foo"), s -> s.toUpperCase());

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}
