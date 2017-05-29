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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxMapTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ANY);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.map(d -> {
					throw exception();
				})),

				scenario(f -> f.map(d -> null))

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.map(d -> d))
		);
	}

	Flux<Integer> just = Flux.just(1);

	@Test(expected = NullPointerException.class)
	public void nullSource() {
		new FluxMap<Integer, Integer>(null, v -> v);
	}

	@Test(expected = NullPointerException.class)
	public void nullMapper() {
		just.map(null);
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
	public void syncFusion() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .map(v -> v + 1)
		    .subscribe(ts);

		ts.assertValues(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void asyncFusion() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		UnicastProcessor<Integer> up =
				UnicastProcessor.Builder.<Integer>create().queue(new ConcurrentLinkedQueue<>()).build();

		up.map(v -> v + 1)
		  .subscribe(ts);

		for (int i = 1; i < 11; i++) {
			up.onNext(i);
		}
		up.onComplete();

		ts.assertValues(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void asyncFusionBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(1);

		UnicastProcessor<Integer> up =
				UnicastProcessor.Builder.<Integer>create().queue(new ConcurrentLinkedQueue<>()).build();

		Flux.just(1)
		    .hide()
		    .flatMap(w -> up.map(v -> v + 1))
		    .subscribe(ts);

		up.onNext(1);

		ts.assertValues(2)
		  .assertNoError()
		  .assertNotComplete();

		up.onComplete();

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mapFilter() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(0, 1_000_000)
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mapFilterBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		Flux.range(0, 1_000_000)
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(250_000)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void hiddenMapFilter() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(0, 1_000_000)
		    .hide()
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void hiddenMapFilterBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		Flux.range(0, 1_000_000)
		    .hide()
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(250_000)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void hiddenMapHiddenFilterBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		Flux.range(0, 1_000_000)
		    .hide()
		    .map(v -> v + 1)
		    .hide()
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(250_000)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertComplete();
	}

    @Test
    public void scanSubscriber() {
        Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxMap.MapSubscriber<Integer, String> test = new FluxMap.MapSubscriber<>(actual, i -> String.valueOf(i));
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }

    @Test
    public void scanConditionalSubscriber() {
        Fuseable.ConditionalSubscriber<String> actual = Mockito.mock(Fuseable.ConditionalSubscriber.class);
        FluxMap.MapConditionalSubscriber<Integer, String> test = new FluxMap.MapConditionalSubscriber<>(actual, i -> String.valueOf(i));
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }

    @Test
    public void scanFuseableSubscriber() {
        Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxMapFuseable.MapFuseableSubscriber<Integer, String> test =
        		new FluxMapFuseable.MapFuseableSubscriber<>(actual, i -> String.valueOf(i));
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }

    @Test
    public void scanFuseableConditionalSubscriber() {
        Fuseable.ConditionalSubscriber<String> actual = Mockito.mock(Fuseable.ConditionalSubscriber.class);
        FluxMapFuseable.MapFuseableConditionalSubscriber<Integer, String> test =
        		new FluxMapFuseable.MapFuseableConditionalSubscriber<>(actual, i -> String.valueOf(i));
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }
}
