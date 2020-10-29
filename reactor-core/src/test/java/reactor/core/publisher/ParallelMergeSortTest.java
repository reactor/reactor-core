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

import java.util.List;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.ParallelMergeSort.MergeSortInner;
import reactor.core.publisher.ParallelMergeSort.MergeSortMain;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Scannable.Attr.RUN_STYLE;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;

public class ParallelMergeSortTest {

	@Test
	public void scanOperator() {
		ParallelFlux<List<Integer>> source = Flux.just(500, 300).buffer(1).parallel(10);
		ParallelMergeSort<Integer> test = new ParallelMergeSort<>(source, Integer::compareTo);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(RUN_STYLE)).isSameAs(SYNC);
	}

	@Test
	public void scanMainSubscriber() {
		LambdaSubscriber<Integer> subscriber = new LambdaSubscriber<>(null, e -> { }, null,
				s -> s.request(2));
		MergeSortMain<Integer> test = new MergeSortMain<>(subscriber, 4, Integer::compareTo);

		subscriber.onSubscribe(test);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(subscriber);
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(2);
		assertThat(test.scan(RUN_STYLE)).isSameAs(SYNC);

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		test.remaining = 3;
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
		test.remaining = 0;
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(4);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanInnerSubscriber() {
		CoreSubscriber<Integer> mainActual = new LambdaSubscriber<>(null, e -> { }, null, null);
		MergeSortMain<Integer> main = new MergeSortMain<>(mainActual, 2, Integer::compareTo);
		MergeSortInner<Integer> test = new MergeSortInner<>(main, 1);

		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(RUN_STYLE)).isSameAs(SYNC);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

}
