/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
public class CoreContextTest {

	@Test
	public void putAllOtherNonAbstract() {
		Context other = new ContextTest.ForeignContext("staticKey", "staticValue");
		Context initial = Context.of(1, "A", 2, "B", 3, "C");
		Context result = initial.putAll(other);

		assertThat(result).isInstanceOf(CoreContext.class)
		                  .isInstanceOf(Context4.class);
		Context4 context4 = (Context4) result;

		assertThat(context4.key1).as("key1").isEqualTo(1);
		assertThat(context4.value1).as("value1").isEqualTo("A");
		assertThat(context4.key2).as("key2").isEqualTo(2);
		assertThat(context4.value2).as("value2").isEqualTo("B");
		assertThat(context4.key3).as("key3").isEqualTo(3);
		assertThat(context4.value3).as("value3").isEqualTo("C");
		assertThat(context4.key4).as("key4").isEqualTo("staticKey");
		assertThat(context4.value4).as("value4").isEqualTo("staticValue");
	}

}