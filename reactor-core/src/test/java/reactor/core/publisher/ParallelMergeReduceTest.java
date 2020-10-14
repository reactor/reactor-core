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

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.ParallelMergeReduce.MergeReduceInner;
import reactor.core.publisher.ParallelMergeReduce.MergeReduceMain;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class ParallelMergeReduceTest {

	@Test
	public void reduceFull() {
		for (int i = 1;
		     i <= Runtime.getRuntime()
		                 .availableProcessors() * 2;
		     i++) {
			AssertSubscriber<Integer> ts = AssertSubscriber.create();

			Flux.range(1, 10)
			    .parallel(i)
			    .reduce((a, b) -> a + b)
			    .subscribe(ts);

			ts.assertValues(55);
		}
	}

	@Test
	public void parallelReduceFull() {
		int m = 100_000;
		for (int n = 1; n <= m; n *= 10) {
//            System.out.println(n);
			for (int i = 1;
			     i <= Runtime.getRuntime()
			                 .availableProcessors();
			     i++) {
//                System.out.println("  " + i);

				Scheduler scheduler = Schedulers.newParallel("test", i);

				try {
					AssertSubscriber<Long> ts = AssertSubscriber.create();

					Flux.range(1, n)
					    .map(v -> (long) v)
					    .parallel(i)
					    .runOn(scheduler)
					    .reduce((a, b) -> a + b)
					    .subscribe(ts);

					ts.await(Duration.ofSeconds(500));

					long e = ((long) n) * (1 + n) / 2;

					ts.assertValues(e);
				}
				finally {
					scheduler.dispose();
				}
			}
		}
	}

	@Test
	public void scanOperator() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel();
		ParallelMergeReduce<Integer> test = new ParallelMergeReduce<>(source, (a, b) -> a + b);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanMainSubscriber() {
		CoreSubscriber<? super Integer> subscriber = new LambdaSubscriber<>(null, e -> { }, null,
				sub -> sub.request(2));
		MergeReduceMain<Integer> test = new MergeReduceMain<>(subscriber, 2, (a, b) -> a + b);

		subscriber.onSubscribe(test);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(subscriber);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		test.innerComplete(1);
		test.innerComplete(2);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanMainSubscriberError() {
		CoreSubscriber<? super Integer> subscriber = new LambdaSubscriber<>(null, e -> { }, null,
				sub -> sub.request(2));
		MergeReduceMain<Integer> test = new MergeReduceMain<>(subscriber, 2, (a, b) -> a + b);

		subscriber.onSubscribe(test);

		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		test.innerError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
	}

	@Test
	public void scanInnerSubscriber() {
		CoreSubscriber<? super Integer> subscriber = new LambdaSubscriber<>(null, e -> { }, null, null);
		MergeReduceMain<Integer> main = new MergeReduceMain<>(subscriber, 2, (a, b) -> a + b);
		MergeReduceInner<Integer> test = new MergeReduceInner<>(main, (a, b) -> a + b);

		Subscription s = Operators.emptySubscription();
		test.onSubscribe(s);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(s);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.done = true;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isZero();
		test.value = 3;
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}
