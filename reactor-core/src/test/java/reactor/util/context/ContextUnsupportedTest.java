/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Map;
import java.util.NoSuchElementException;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@RunWith(JUnitParamsRunner.class)
public class ContextUnsupportedTest {

	String cause;

	@Before
	public void initUnsupported() {
		cause = "ContextUnsupportedTest";
	}

	@Test
	@Parameters({"true", "false"})
	public void putAnyKeyContext1(boolean fatal) {
		ContextUnsupported c = new ContextUnsupported(cause, fatal);

		Context put = c.put(1, "Abis");
		assertThat(put)
				.isInstanceOf(Context1.class);
		assertThat(put.stream().map(Map.Entry::getKey))
				.containsExactly(1);
		assertThat(put.stream().map(Map.Entry::getValue))
				.containsExactly("Abis");
	}

	@Test
	@Parameters({"true", "false"})
	public void isEmpty(boolean fatal) {
		ContextUnsupported c = new ContextUnsupported(cause, fatal);

		assertThat(c.isEmpty()).as("unsupported().isEmpty()").isTrue();
		assertThat(c.isEmpty()).as("new ContextUnsupported().isEmpty()").isTrue();
	}

	@Test
	@Parameters({"true", "false"})
	public void hasKey(boolean fatal) {
		ContextUnsupported c = new ContextUnsupported(cause, fatal);

		assertThat(c.hasKey(1)).as("hasKey(1)").isFalse();
	}

	@Test
	@Parameters({"true", "false"})
	public void removeKeys(boolean fatal) {
		ContextUnsupported c = new ContextUnsupported(cause, fatal);

		assertThat(c.delete(1)).isSameAs(c);
	}

	@Test
	public void get_fatalThrows() {
		ContextUnsupported c = new ContextUnsupported(cause, true);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.get(1))
				.withMessage("Context#get(Object) is not supported due to Context-incompatible element in the chain")
				.withCause(new RuntimeException(cause));
	}

	@Test
	public void get_Throws() {
		ContextUnsupported c = new ContextUnsupported(cause, false);

		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(1))
				.withMessage("Context is empty");
		//the log is left out but should contain an ERROR:
		// "Context#get(Object) is not supported due to Context-incompatible element in the chain: ContextUnsupportedTest");
	}

	@Test
	public void getClass_fatalThrows() {
		ContextUnsupported c = new ContextUnsupported(cause, true);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.get(String.class))
				.withMessage("Context#get(Class) is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void getClass_throws() {
		ContextUnsupported c = new ContextUnsupported(cause, false);

		assertThatExceptionOfType(NoSuchElementException.class)
				.isThrownBy(() -> c.get(String.class))
				.withMessage("Context does not contain a value of type java.lang.String");
	}

	@Test
	public void getOrEmpty_fatalThrows() {
		ContextUnsupported c = new ContextUnsupported(cause, true);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.getOrEmpty(1))
		.withMessage("Context#getOrEmpty is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void getOrEmpty_empty() {
		ContextUnsupported c = new ContextUnsupported(cause, false);

		assertThat(c.getOrEmpty(1))
				.isEmpty();
	}

	@Test
	public void getOrDefaultWithDefault_fatalThrows() {
		ContextUnsupported c = new ContextUnsupported(cause, true);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.getOrDefault("peeka", "boo"))
		.withMessage("Context#getOrDefault is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void getOrDefaultWithDefault_default() {
		ContextUnsupported c = new ContextUnsupported(cause, false);

		assertThat(c.getOrDefault("peeka", "boo"))
				.isEqualTo("boo");
	}

	@Test
	public void getOrDefaultWithDefaultNull_fatalThrows() {
		ContextUnsupported c = new ContextUnsupported(cause, true);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.getOrDefault("peeka", null))
				.withMessage("Context#getOrDefault is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void getOrDefaultWithDefaultNull_null() {
		ContextUnsupported c = new ContextUnsupported(cause, false);

		assertThat(c.getOrDefault("peeka", (Object) null))
				.isNull();
	}

	@Test
	public void stream_fatalThrows() {
		ContextUnsupported c = new ContextUnsupported(cause, true);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> c.stream())
		.withMessage("Context#stream is not supported due to Context-incompatible element in the chain");
	}

	@Test
	public void stream_empty() {
		ContextUnsupported c = new ContextUnsupported(cause, false);

		assertThat(c.stream()).isEmpty();
	}

	@Test
	@Parameters({"true", "false"})
	public void string(boolean fatal) {
		ContextUnsupported c = new ContextUnsupported(cause, fatal);

		assertThat(c.toString()).isEqualTo("ContextUnsupported{}");
	}

	@Test
	public void unsupportedApi() {

		assertThat(Context.unsupported("message"))
				.isInstanceOf(ContextUnsupported.class)
				.hasToString("ContextUnsupported{}");
	}

	@Test
	@Parameters({"true", "false"})
	public void putAllOf(boolean fatal) {
		ContextUnsupported c = new ContextUnsupported(cause, fatal);

		Context m = Context.of("A", 1, "B", 2, "C", 3);
		Context put = c.putAll(m);

		assertThat(put).isInstanceOf(Context3.class)
		               .hasToString("Context3{A=1, B=2, C=3}");
	}

	@Test
	@Parameters({"true", "false"})
	public void putAllOfEmpty(boolean fatal) {
		ContextUnsupported c = new ContextUnsupported(cause, fatal);

		Context m = Context.empty();
		Context put = c.putAll(m);

		assertThat(put).isSameAs(c);
	}

}