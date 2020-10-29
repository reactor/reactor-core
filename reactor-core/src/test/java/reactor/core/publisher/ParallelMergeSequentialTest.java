/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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
import reactor.core.publisher.ParallelMergeSequential.MergeSequentialInner;
import reactor.core.publisher.ParallelMergeSequential.MergeSequentialMain;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;

public class ParallelMergeSequentialTest {

	@Test
	public void scanOperator() {
		ParallelFlux<Integer> source = Flux.just(500, 300).parallel(10);
		ParallelMergeSequential<Integer> test = new ParallelMergeSequential<>(source, 123, Queues.one());

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanMainSubscriber() {
		LambdaSubscriber<Integer> subscriber = new LambdaSubscriber<>(null, e -> { }, null,
				s -> s.request(2));
		MergeSequentialMain<Integer>
				test = new MergeSequentialMain<>(subscriber, 4, 123, Queues.small());

		subscriber.onSubscribe(test);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(subscriber);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(2);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanMainSubscriberDoneAfterNComplete() {
		LambdaSubscriber<Integer> subscriber = new LambdaSubscriber<>(null, e -> { }, null,
				s -> s.request(2));

		int n = 4;

		MergeSequentialMain<Integer>
				test = new MergeSequentialMain<>(subscriber, n, 123, Queues.small());

		subscriber.onSubscribe(test);

		for (int i = 0; i < n; i++) {
			assertThat(test.scan(Scannable.Attr.TERMINATED)).as("complete %d", i)
			                                                .isFalse();
			test.onComplete();
		}

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanMainSubscriberError() {
		LambdaSubscriber<Integer> subscriber = new LambdaSubscriber<>(null, e -> { }, null,
				s -> s.request(2));
		MergeSequentialMain<Integer>
				test = new MergeSequentialMain<>(subscriber, 4, 123, Queues.small());

		subscriber.onSubscribe(test);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
	}

	@Test
	public void scanInnerSubscriber() {
		CoreSubscriber<Integer>
				mainActual = new LambdaSubscriber<>(null, e -> { }, null, null);
		MergeSequentialMain<Integer> main = new MergeSequentialMain<>(mainActual, 2,  123, Queues.small());
		MergeSequentialInner<Integer> test = new MergeSequentialInner<>(main, 456);

		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(456);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

}
