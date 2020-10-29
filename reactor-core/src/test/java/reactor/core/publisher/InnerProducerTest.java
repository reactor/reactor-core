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
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class InnerProducerTest {

	@Test
	public void scanDefaultMethod() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, null, null, null);
		InnerProducer<String> test = new InnerProducer<String>() {
			@Override
			public CoreSubscriber<? super String> actual() {
				return actual;
			}

			@Override
			public void request(long n) { }

			@Override
			public void cancel() { }
		};

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
	}
}
