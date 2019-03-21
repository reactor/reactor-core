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

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class SerializedSubscriberTest {

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