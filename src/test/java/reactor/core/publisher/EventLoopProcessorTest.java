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

import java.util.concurrent.Executors;

import org.junit.Test;
import reactor.core.Scannable;
import reactor.util.concurrent.WaitStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Scannable.BooleanAttr.TERMINATED;
import static reactor.core.Scannable.ScannableAttr.PARENT;
import static reactor.core.Scannable.ThrowableAttr.ERROR;

public class EventLoopProcessorTest {

	@Test
	public void scanMain() throws Exception {
		EventLoopProcessor<String> test = new EventLoopProcessor<String>(128,
				r -> new Thread(r),
				Executors.newSingleThreadExecutor(),
				true,
				false,
				() -> null,
				WaitStrategy.sleeping()) {
			@Override
			public void run() {

			}

			@Override
			public long getPending() {
				return 456;
			}

			@Override
			void doError(Throwable throwable) {
				this.error = throwable;
			}
		};

		assertThat(test.scan(PARENT)).isNull();
		assertThat(test.scan(TERMINATED)).isFalse();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(TERMINATED)).isTrue();
		assertThat(test.scan(ERROR)).hasMessage("boom");

		assertThat(test.scan(Scannable.IntAttr.CAPACITY)).isEqualTo(128);
	}

}