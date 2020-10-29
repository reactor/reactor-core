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
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class BlockingSingleSubscriberTest {

	BlockingSingleSubscriber<Object> test = new BlockingSingleSubscriber<Object>() {
		@Override
		public void onNext(Object o) { }

		@Override
		public void onError(Throwable t) {
			value = null;
			error = t;
			countDown();
		}
	};

	@Test
	public void scanMain() {
		Subscription s = Operators.emptySubscription();
		test.onSubscribe(s);

		assertThat(test.scan(Scannable.Attr.PARENT)).describedAs("PARENT").isSameAs(s);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).describedAs("TERMINATED").isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).describedAs("CANCELLED").isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).describedAs("ERROR").isNull();
		assertThat(test.scan(Scannable.Attr.PREFETCH)).describedAs("PREFETCH").isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).describedAs("RUN_STYLE").isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanMainTerminated() {
		test.onComplete();

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanMainError() {
		test.onError(new IllegalStateException("boom"));

		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanMainCancelled() {
		test.dispose();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
	}
}
