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

import java.util.List;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class FluxRepeatPredicateTest {

	@Test
	public void predicateNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never()
					.repeat(null);
		});
	}

	@Test
	public void nMinusOne() {
		Flux<Integer> source = Flux.just(1, 2, 3);

		assertThatIllegalArgumentException()
		          .isThrownBy(() -> source.repeat(-1, () -> true))
		          .withMessage("numRepeat >= 0 required");
	}

	@Test
	public void nZero() {
		StepVerifier.create(Flux.just(1, 2, 3)
				.repeat(0, () -> true))
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	public void nOne() {
		StepVerifier.create(Flux.just(1, 2, 3)
				.repeat(1, () -> true))
		            .expectNext(1, 2, 3)
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	public void nTwo() {
		StepVerifier.create(Flux.just(1, 2, 3)
				.repeat(2, () -> true))
		            .expectNext(1, 2, 3)
		            .expectNext(1, 2, 3)
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	public void normal() {
		int[] times = {1};

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .repeat(() -> times[0]-- > 0)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		int[] times = {1};

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .repeat(() -> times[0]-- > 0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void dontRepeat() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .repeat(() -> false)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void predicateThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .repeat(() -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void alwaysTrueWithNSimilarToSimpleN() {
		List<Integer> expected = Flux.just(1, 2, 3).repeat(3).collectList().block();
		List<Integer> result = Flux.just(1, 2, 3).repeat(3, () -> true).collectList().block();

		assertThat(result).containsExactlyElementsOf(expected);
	}

	@Test
	public void alwaysFalseWithNSimilarToSimpleZero() {
		List<Integer> expected = Flux.just(1, 2, 3).repeat(0).collectList().block();
		List<Integer> result = Flux.just(1, 2, 3).repeat(3, () -> false).collectList().block();

		assertThat(result).containsExactlyElementsOf(expected);
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxRepeatPredicate<Integer> test = new FluxRepeatPredicate<>(parent, () -> true);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber(){
		Flux<Integer> source = Flux.just(1);
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxRepeatPredicate.RepeatPredicateSubscriber<Integer> test =
				new FluxRepeatPredicate.RepeatPredicateSubscriber<>(source, actual,  () -> true);

		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}
