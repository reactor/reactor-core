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
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class ParallelThenTest {

	@Test
	public void thenFull() {
		for (int i = 1;
		     i <= Runtime.getRuntime()
		                 .availableProcessors() * 2;
		     i++) {

			Flux.range(1, 10)
			    .parallel(i)
			    .then()
			    .as(StepVerifier::create)
			    .verifyComplete();
		}
	}

	@Test
	public void parallelThenFull() {
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
					Flux.range(1, n)
					    .map(v -> (long) v)
					    .parallel(i)
					    .runOn(scheduler)
					    .then()
					    .as(StepVerifier::create)
					    .verifyComplete();
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
		ParallelThen test = new ParallelThen(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanMainSubscriber() {
		CoreSubscriber<? super Void> subscriber = new LambdaSubscriber<>(null, e -> { }, null,
				sub -> sub.request(2));
		ParallelThen.ThenMain test = new ParallelThen.ThenMain(subscriber, 2);

		subscriber.onSubscribe(test);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(subscriber);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		test.innerComplete();
		test.innerComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanMainSubscriberError() {
		CoreSubscriber<? super Void> subscriber = new LambdaSubscriber<>(null, e -> { }, null,
				sub -> sub.request(2));
		ParallelThen.ThenMain test = new ParallelThen.ThenMain(subscriber, 2);

		subscriber.onSubscribe(test);

		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		test.innerError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
	}

	@Test
	public void scanInnerSubscriber() {
		CoreSubscriber<? super Void> subscriber = new LambdaSubscriber<>(null, e -> { }, null, null);
		ParallelThen.ThenMain main = new ParallelThen.ThenMain(subscriber, 2);
		ParallelThen.ThenInner test = new ParallelThen.ThenInner(main);

		Subscription s = Operators.emptySubscription();
		test.onSubscribe(s);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(s);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}
