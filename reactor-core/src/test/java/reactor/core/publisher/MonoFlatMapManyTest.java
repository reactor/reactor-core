/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoFlatMapManyTest {

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1).hide().flatMapMany(v -> Flux.just(2).hide())
		.subscribe(ts);

		ts.assertValues(2)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalInnerJust() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1).hide().flatMapMany(v -> Flux.just(2))
		.subscribe(ts);

		ts.assertValues(2)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalInnerEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1).hide().flatMapMany(v -> Flux.<Integer>empty())
		.subscribe(ts);

		ts.assertNoValues()
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void scanMain() {
		Subscriber<Integer> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoFlatMapMany.FlatMapManyMain<String, Integer> test = new MonoFlatMapMany.FlatMapManyMain<>
				(actual, s -> Flux.just(1, 2, 3), Context.empty());
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
	}

	@Test
	public void scanInner() {
		Subscriber<Integer> mainActual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		Subscriber<Integer> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoFlatMapMany.FlatMapManyMain<String, Integer> main = new MonoFlatMapMany.FlatMapManyMain<>
				(mainActual, s -> Flux.just(1, 2, 3), Context.empty());
		MonoFlatMapMany.FlatMapManyInner<Integer> test = new MonoFlatMapMany.FlatMapManyInner<>(main,
				actual);
		Subscription innerSubscription = Operators.emptySubscription();
		test.onSubscribe(innerSubscription);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(innerSubscription);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);

		main.requested = 3L;
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(3L);
	}

}
