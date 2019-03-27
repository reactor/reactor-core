/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

package reactor.test;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CustomizableObjectFormatterTest {

	//TODO test with unwrap/catchAll/specific combinations

	@Test
	public void convertVarArgs() {
		CustomizableObjectFormatter converter = new CustomizableObjectFormatter();
		converter.setCatchAll(o -> o.getClass().getSimpleName());

		Object[] converted = CustomizableObjectFormatter.convertVarArgs(converter,"A", 1L, null);

		assertThat(converted).containsExactly("String", "Long", "null");
	}

	@Test
	public void convertVarArgsEmpty() {
		CustomizableObjectFormatter converter = new CustomizableObjectFormatter();
		converter.setCatchAll(o -> o.getClass().getSimpleName());

		Object[] converted = CustomizableObjectFormatter.convertVarArgs(converter);

		assertThat(converted).isEmpty();
	}

	@Test
	public void convertVarArgsContainsNullOnly() {
		CustomizableObjectFormatter converter = new CustomizableObjectFormatter();
		converter.setCatchAll(o -> o.getClass().getSimpleName());

		Object[] converted = CustomizableObjectFormatter.convertVarArgs(converter, (Object) null);

		assertThat(converted).containsExactly("null");
	}

	@Test
	public void convertVarArgsNull() {
		CustomizableObjectFormatter converter = new CustomizableObjectFormatter();
		converter.setCatchAll(o -> o.getClass().getSimpleName());

		Object[] converted = CustomizableObjectFormatter.convertVarArgs(converter, (Object[]) null);

		assertThat(converted).isNull();
	}
}