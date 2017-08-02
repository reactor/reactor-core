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
package reactor.core;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.junit.Before;
import org.junit.Test;
import reactor.test.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephane Maldini
 */
public class ExceptionsTest {

	@Test
	public void testWrapping() throws Exception {

		Throwable t = new Exception("test");

		Throwable w = Exceptions.bubble(Exceptions.propagate(t));

		assertTrue(Exceptions.unwrap(w) == t);
	}

	@Test
	public void allOverflowIsIllegalState() {
		IllegalStateException overflow1 = Exceptions.failWithOverflow();
		IllegalStateException overflow2 = Exceptions.failWithOverflow("foo");

		assertTrue(Exceptions.isOverflow(overflow1));
		assertTrue(Exceptions.isOverflow(overflow2));
	}

	@Test
	public void allIllegalStateIsntOverflow() {
		IllegalStateException ise = new IllegalStateException("foo");

		assertFalse(Exceptions.isOverflow(ise));
	}

	@Test
	public void multipleWithNullVararg() {
		//noinspection ConstantConditions
		assertThat(Exceptions.multiple((Throwable[]) null))
				.isInstanceOf(RuntimeException.class)
				.hasMessage("Multiple exceptions")
	            .hasNoSuppressedExceptions();
	}

	@Test
	public void multipleWithOneVararg() {
		IOException e1 = new IOException("boom");

		assertThat(Exceptions.multiple(e1))
				.isInstanceOf(RuntimeException.class)
				.hasMessage("Multiple exceptions")
	            .hasSuppressedException(e1);
	}

	@Test
	public void multipleWithTwoVararg() {
		IOException e1 = new IOException("boom");
		IllegalArgumentException  e2 = new IllegalArgumentException("boom");

		assertThat(Exceptions.multiple(e1, e2))
				.isInstanceOf(RuntimeException.class)
				.hasMessage("Multiple exceptions")
	            .hasSuppressedException(e1)
	            .hasSuppressedException(e2);
	}

	@Test
	public void isMultiple() {
		Exception e1 = new IllegalStateException("1");
		Exception e2 = new IllegalArgumentException("2");

		Exception composite = Exceptions.multiple(e1, e2);

		assertThat(Exceptions.isMultiple(composite)).isTrue();
		assertThat(Exceptions.isMultiple(Exceptions.failWithCancel())).isFalse();
		assertThat(Exceptions.isMultiple(null)).isFalse();
	}

	@Test
	public void unwrapMultipleNull() {
		assertThat(Exceptions.unwrapMultiple(null))
				.isEmpty();
	}

	@Test
	public void unwrapMultipleNotComposite() {
		RuntimeException e1 = Exceptions.failWithCancel();
		assertThat(Exceptions.unwrapMultiple(e1)).containsExactly(e1);
	}

	volatile Throwable addThrowable;
	static final AtomicReferenceFieldUpdater<ExceptionsTest, Throwable> ADD_THROWABLE =
			AtomicReferenceFieldUpdater.newUpdater(ExceptionsTest.class, Throwable.class, "addThrowable");

	@Before
	public void resetAddThrowable() {
		addThrowable = null;
	}

	@Test
	public void addThrowable() {
		Throwable e1 = new IllegalStateException("add1");
		Throwable e2 = new IllegalArgumentException("add2");
		Throwable e3 = new OutOfMemoryError("add3");

		assertThat(addThrowable).isNull();

		Exceptions.addThrowable(ADD_THROWABLE, this, e1);

		assertThat(addThrowable).isSameAs(e1);

		Exceptions.addThrowable(ADD_THROWABLE, this, e2);

		assertThat(Exceptions.isMultiple(addThrowable)).isTrue();
		assertThat(addThrowable)
				.hasSuppressedException(e1)
				.hasSuppressedException(e2);

		Exceptions.addThrowable(ADD_THROWABLE, this, e3);

		assertThat(Exceptions.isMultiple(addThrowable)).isTrue();
		assertThat(addThrowable)
				.hasSuppressedException(e1)
				.hasSuppressedException(e2)
				.hasSuppressedException(e3);
	}

	@Test
	public void addThrowableRace() throws Exception {
		for (int i = 0; i < 10; i++) {
			final int idx = i;
			RaceTestUtils.race(
					() -> Exceptions.addThrowable(ADD_THROWABLE, ExceptionsTest.this, new IllegalStateException("boomState" + idx)),
					() -> Exceptions.addThrowable(ADD_THROWABLE, ExceptionsTest.this, new IllegalArgumentException("boomArg" + idx))
			);
		}

		assertThat(addThrowable.getSuppressed())
				.hasSize(20);
	}
}