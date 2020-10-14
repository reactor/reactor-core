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

package reactor.util.context;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ContextViewTest {

	// == tests for default methods ==

	@Test
	public void getWithClass() {
		ContextView context = Context.of(String.class, "someString");

		assertThat(context.get(String.class)).isEqualTo("someString");
	}

	@Test
	public void getWithClassKeyButNonMatchingInstance() {
		ContextView context = Context.of(String.class, 4);

		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> context.get(String.class))
				.withMessage("Context does not contain a value of type java.lang.String");
	}

	@Test
	public void getOrEmptyWhenMatch() {
		ContextView context = Context.of(1, "A");

		assertThat(context.getOrEmpty(1)).hasValue("A");
	}

	@Test
	public void getOrEmptyWhenNoMatch() {
		ContextView context = Context.of(1, "A");

		assertThat(context.getOrEmpty(2)).isEmpty();
	}
}
