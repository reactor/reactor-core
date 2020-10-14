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
	public void scanOperator(){
	    MonoFlatMapMany<String, Integer> test = new MonoFlatMapMany<>(Mono.just("foo"), s -> Mono.just(1));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanMain() {
		CoreSubscriber<Integer> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoFlatMapMany.FlatMapManyMain<String, Integer> test = new MonoFlatMapMany.FlatMapManyMain<>
				(actual, s -> Flux.just(1, 2, 3));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanInner() {
		CoreSubscriber<Integer> mainActual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		CoreSubscriber<Integer> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoFlatMapMany.FlatMapManyMain<String, Integer> main = new MonoFlatMapMany.FlatMapManyMain<>
				(mainActual, s -> Flux.just(1, 2, 3));
		MonoFlatMapMany.FlatMapManyInner<Integer> test = new MonoFlatMapMany.FlatMapManyInner<>(main,
				actual);
		Subscription innerSubscription = Operators.emptySubscription();
		test.onSubscribe(innerSubscription);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(innerSubscription);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		main.requested = 3L;
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(3L);
	}

}
