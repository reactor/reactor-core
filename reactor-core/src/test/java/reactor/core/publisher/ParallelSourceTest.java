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
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;

public class ParallelSourceTest {

	@Test
	public void parallelism() {
		Flux<String> source = Flux.empty();
		ParallelSource<String> test = new ParallelSource<>(source, 100, 123, Queues.small());
		assertThat(test.parallelism()).isEqualTo(100);
	}

	@Test
	public void scanOperator() throws Exception {
		Flux<String> source = Flux.just("").map(i -> i);
		ParallelSource<String> test = new ParallelSource<>(source, 100, 123,
				Queues.small());

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanMainSubscriber() {
		@SuppressWarnings("unchecked")
		CoreSubscriber<String>[] subs = new CoreSubscriber[1];
		subs[0] = new LambdaSubscriber<>(null, e -> {}, null, null);
		ParallelSource.ParallelSourceMain<String> test = new ParallelSource.ParallelSourceMain<>(
				subs, 123, Queues.one());

		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isZero();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		test.queue.offer("foo");
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanInnerSubscriber() {
		@SuppressWarnings("unchecked")
		CoreSubscriber<String>[] subs = new CoreSubscriber[2];
		subs[0] = new LambdaSubscriber<>(null, e -> {}, null, null);
		subs[1] = new LambdaSubscriber<>(null, e -> {}, null, null);
		ParallelSource.ParallelSourceMain<String> main = new ParallelSource.ParallelSourceMain<>(
				subs, 123, Queues.one());

		ParallelSource.ParallelSourceMain.ParallelSourceInner<String> test =
				new ParallelSource.ParallelSourceMain.ParallelSourceInner<>(
						main, 1, 10);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(subs[test.index]);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}
