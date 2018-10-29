/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.function;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class TriFunctionTest {

	@Test
	public void andThenNullRejected() {
		TriFunction<String, String, String, List<String>> triFunction =
				(a, b, c) -> Arrays.asList(a, b, c);

		assertThatNullPointerException()
				.isThrownBy(() -> triFunction.andThen(null))
				.withMessage("The 'after' Function must not be null");
	}

	@Test
	public void andThenAppliedLast() {
		TriFunction<String, String, String, String> baseFunction =
				(a, b, c) -> a + b + c;
		TriFunction<String, String, String, String> triFunction = baseFunction.andThen(s -> s + "last");

		assertThat(triFunction.apply("foo", "bar", "baz"))
				.isEqualTo("foobarbazlast");
	}
}