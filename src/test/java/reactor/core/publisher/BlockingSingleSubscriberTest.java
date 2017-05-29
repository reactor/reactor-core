/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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

public class BlockingSingleSubscriberTest {

	BlockingSingleSubscriber test = new BlockingSingleSubscriber() {
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

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).describedAs("PARENT").isSameAs(s);
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).describedAs("TERMINATED").isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).describedAs("CANCELLED").isFalse();
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).describedAs("ERROR").isNull();
		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).describedAs("PREFETCH").isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void scanMainTerminated() {
		test.onComplete();

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanMainError() {
		test.onError(new IllegalStateException("boom"));

		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanMainCancelled() {
		test.dispose();

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
	}
}