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

package reactor.util.context;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ContextTest {

	@Test
	public void empty() throws Exception {
		Context c = Context.empty();

		assertThat(c).isInstanceOf(Context0.class);
		assertThat(c.isEmpty()).as("isEmpty").isTrue();
	}

	@Test
	public void of1() throws Exception {
		Context c = Context.of(1, 100);

		assertThat(c).isInstanceOf(Context1.class);
		assertThat(c.isEmpty()).as("isEmpty").isFalse();
		assertThat(c.stream()).hasSize(1);
	}

	@Test
	public void of2() throws Exception {
		Context c = Context.of(1, 100, 2, 200);

		assertThat(c).isInstanceOf(Context2.class);
		assertThat(c.isEmpty()).as("isEmpty").isFalse();
		assertThat(c.stream()).hasSize(2);
	}

	@Test
	public void of3() throws Exception {
		Context c = Context.of(1, 100, 2, 200, 3, 300);

		assertThat(c).isInstanceOf(Context3.class);
		assertThat(c.isEmpty()).as("isEmpty").isFalse();
		assertThat(c.stream()).hasSize(3);
	}

	@Test
	public void of4() throws Exception {
		Context c = Context.of(1, 100, 2, 200, 3, 300, 4, 400);

		assertThat(c).isInstanceOf(Context4.class);
		assertThat(c.isEmpty()).as("isEmpty").isFalse();
		assertThat(c.stream()).hasSize(4);
	}

	@Test
	public void of5() throws Exception {
		Context c = Context.of(1, 100, 2, 200, 3, 300, 4, 400, 5, 500);

		assertThat(c).isInstanceOf(Context5.class);
		assertThat(c.isEmpty()).as("isEmpty").isFalse();
		assertThat(c.stream()).hasSize(5);
	}

}