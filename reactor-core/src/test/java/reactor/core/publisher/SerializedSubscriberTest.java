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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SerializedSubscriberTest {

	@Test
	public void onNextRaceWithCancelDoesNotLeak() {
		int i = 0;
		do {
			CopyOnWriteArrayList<Object> discarded = new CopyOnWriteArrayList<>();

			AssertSubscriber<Integer> consumer = new AssertSubscriber<>(
					Operators.enableOnDiscard(Context.empty(), discarded::add),
					Long.MAX_VALUE);

			SerializedSubscriber<Integer> leaky = new SerializedSubscriber<>(consumer);
			leaky.onSubscribe(Operators.emptySubscription());

			leaky.onNext(1);
			RaceTestUtils.race(
					() -> {
						leaky.onNext(2);
						leaky.onNext(4);
					},
					() -> {
						leaky.onNext(3);
						leaky.cancel();
					}
			);

			if (consumer.values().size() == 4) {
				assertThat(discarded).as("when consumed all, none discarded").isEmpty();
				System.out.println("consumed all, looping once more");
				continue;
			}

			if (discarded.size() == 4) {
				assertThat(consumer.values()).as("when discarded all, none consumed").isEmpty();
				System.out.println("discarded all, looping once more");
				continue;
			}

			try {
				assertThat(discarded.size() + consumer.values().size())
						.as("elements discarded or passed down in round #%s: <%s> and <%s>", i, discarded, consumer.values())
						.isEqualTo(4);
			} catch (AssertionError assertionError) {
				throw assertionError;
			}

			i++;
		} while (i < 1000);
	}


	@Test
	public void scanSerializedSubscriber() {
		LambdaSubscriber<String> actual = new LambdaSubscriber<>(null, e -> { }, null, null);
		SerializedSubscriber<String> test = new SerializedSubscriber<>(actual);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isZero();
		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(SerializedSubscriber.LinkedArrayNode.DEFAULT_CAPACITY);

		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanSerializedSubscriberMaxBuffered() {
		LambdaSubscriber<String> actual = new LambdaSubscriber<>(null, e -> { }, null, null);
		SerializedSubscriber<String> test = new SerializedSubscriber<>(actual);

		test.tail = new SerializedSubscriber.LinkedArrayNode<>("");
		test.tail.count = Integer.MAX_VALUE;

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.LARGE_BUFFERED)).isNull();
	}

}