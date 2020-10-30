/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Operators;
import reactor.test.AssertionsUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Simon Baslé
 */
class QueueSubscriptionTest {

	@Test
	void assertJShouldProperlyFormatQueueSubscription() {
		//on purpose, doesn't set any particular representation for QueueSubscriptions
		Fuseable.QueueSubscription<?> queueSubscription = new Fuseable.QueueSubscription<Object>() {
			@Override
			public int requestFusion(int requestedMode) {
				return 0;
			}

			@Override
			public Object poll() {
				return null;
			}

			@Override
			public int size() {
				return 0;
			}

			@Override
			public boolean isEmpty() {
				return false;
			}

			@Override
			public void clear() {

			}

			@Override
			public void request(long n) {

			}

			@Override
			public void cancel() {

			}

			@Override
			public String toString() {
				return "ThisIsNotAQueue";
			}
		};

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> {
					assertThat(queueSubscription).isNull();
				})
		.withMessage("\n" + "Expecting:\n" + " <ThisIsNotAQueue>\n" + "to be equal to:\n" + " <null>\n" + "but was not.");
	}

}
