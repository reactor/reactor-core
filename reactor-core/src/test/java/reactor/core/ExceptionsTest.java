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
package reactor.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;
import reactor.test.util.RaceTestUtils;
import reactor.util.annotation.Nullable;

import static org.assertj.core.api.Assertions.*;
import static reactor.core.Exceptions.*;

/**
 * @author Stephane Maldini
 */
public class ExceptionsTest {

	//used for two addThrowableXxx tests lower in the class. each test receiving a separate instance of ExceptionsTests,
	//there is no need to reset it.
	volatile @Nullable Throwable addThrowable;
	static final AtomicReferenceFieldUpdater<ExceptionsTest, Throwable> ADD_THROWABLE =
			AtomicReferenceFieldUpdater.newUpdater(ExceptionsTest.class, Throwable.class, "addThrowable");

	@Test
	public void bubble() throws Exception {
		Throwable t = new Exception("test");

		Throwable w = Exceptions.bubble(Exceptions.propagate(t));

		assertThat(Exceptions.unwrap(w)).isSameAs(t);
	}

	@Test
	public void nullBubble() throws Exception {
		Throwable w = Exceptions.bubble(null);

		assertThat(Exceptions.unwrap(w)).isSameAs(w);
	}

	@Test
	public void duplicateOnSubscribeReferencesSpec() {
		IllegalStateException error = duplicateOnSubscribeException();
		assertThat(error).hasMessageContaining("Rule 2.12");
	}

	@Test
	public void duplicateOnSubscribeCreatesNewInstances() {
		IllegalStateException error1 = duplicateOnSubscribeException();
		IllegalStateException error2 = duplicateOnSubscribeException();

		assertThat(error1).isNotSameAs(error2);
	}

	@Test
	public void errorCallbackNotImplementedRejectsNull() {
		//noinspection ThrowableNotThrown,ConstantConditions
		assertThatNullPointerException().isThrownBy(() -> Exceptions.errorCallbackNotImplemented(null));
	}

	@Test
	public void isErrorCallbackNotImplemented() {
		UnsupportedOperationException vanillaUnsupported = new UnsupportedOperationException("not error callback");
		UnsupportedOperationException t = errorCallbackNotImplemented(new IllegalStateException("in error callback"));

		assertThat(Exceptions.isErrorCallbackNotImplemented(vanillaUnsupported))
				.as("not error callback")
				.isFalse();
		assertThat(Exceptions.isErrorCallbackNotImplemented(t))
				.as("error callback")
				.isTrue();
	}

	@Test
	public void allOverflowIsIllegalState() {
		IllegalStateException overflow1 = Exceptions.failWithOverflow();
		IllegalStateException overflow2 = Exceptions.failWithOverflow("foo");

		assertThat(Exceptions.isOverflow(overflow1)).isTrue();
		assertThat(Exceptions.isOverflow(overflow2)).isTrue();
	}

	@Test
	public void allIllegalStateIsntOverflow() {
		IllegalStateException ise = new IllegalStateException("foo");

		assertThat(Exceptions.isOverflow(ise)).isFalse();
	}

	@Test
	public void failWithRejectedIsSingleton() {
		assertThat(Exceptions.failWithRejected())
				.isSameAs(Exceptions.failWithRejected())
				.isNotSameAs(Exceptions.failWithRejectedNotTimeCapable());
	}

	@Test
	public void failWithRejectedNotTimeCapableIsSingleton() {
		assertThat(Exceptions.failWithRejectedNotTimeCapable())
				.isSameAs(Exceptions.failWithRejectedNotTimeCapable())
				.isNotSameAs(Exceptions.failWithRejected());
	}

	@Test
	public void isBubbling() {
		Throwable notBubbling = new ReactiveException("foo");
		Throwable bubbling = new BubblingException("foo");

		assertThat(Exceptions.isBubbling(notBubbling)).as("not bubbling").isFalse();
		assertThat(Exceptions.isBubbling(bubbling)).as("bubbling").isTrue();
	}

	@Test
	public void isCancelAndCancelIsBubbling() {
		Throwable notCancel = new BubblingException("foo");
		Throwable cancel = new CancelException();

		assertThat(Exceptions.isCancel(notCancel)).as("not cancel").isFalse();
		assertThat(Exceptions.isCancel(cancel)).as("cancel").isTrue();

		assertThat(Exceptions.isBubbling(cancel)).as("cancel are bubbling").isTrue();
	}

	@Test
	public void nullOrNegativeRequestReferencesSpec() {
		assertThat(Exceptions.nullOrNegativeRequestException(-3))
				.hasMessage("Spec. Rule 3.9 - Cannot request a non strictly positive number: -3");
	}

	@Test
	public void propagateDoesntWrapRuntimeException() {
		Throwable t = new RuntimeException("expected");
		assertThat(Exceptions.propagate(t)).isSameAs(t);
	}

	//TODO test terminate

	@Test
	public void throwIfFatalThrowsBubbling() {
		BubblingException expected = new BubblingException("expected");

		assertThatExceptionOfType(BubblingException.class)
				.isThrownBy(() -> Exceptions.throwIfFatal(expected))
				.isSameAs(expected);
	}

	@Test
	public void throwIfFatalThrowsErrorCallbackNotImplemented() {
		ErrorCallbackNotImplemented expected = new ErrorCallbackNotImplemented(new IllegalStateException("expected cause"));

		assertThatExceptionOfType(ErrorCallbackNotImplemented.class)
				.isThrownBy(() -> Exceptions.throwIfFatal(expected))
				.isSameAs(expected)
				.withCause(expected.getCause());
	}

	@Test
	public void throwIfJvmFatal() {
		VirtualMachineError fatal1 = new VirtualMachineError() {};
		ThreadDeath fatal2 = new ThreadDeath();
		LinkageError fatal3 = new LinkageError();

		assertThatExceptionOfType(VirtualMachineError.class)
				.as("VirtualMachineError")
				.isThrownBy(() -> Exceptions.throwIfJvmFatal(fatal1))
				.isSameAs(fatal1);

		assertThatExceptionOfType(ThreadDeath.class)
				.as("ThreadDeath")
				.isThrownBy(() -> Exceptions.throwIfJvmFatal(fatal2))
				.isSameAs(fatal2);

		assertThatExceptionOfType(LinkageError.class)
				.as("LinkageError")
				.isThrownBy(() -> Exceptions.throwIfJvmFatal(fatal3))
				.isSameAs(fatal3);
	}

	@Test
	public void multipleWithNullVararg() {
		//noinspection ConstantConditions
		assertThat(Exceptions.multiple((Throwable[]) null))
				.isInstanceOf(RuntimeException.class)
				.isExactlyInstanceOf(CompositeException.class)
				.hasMessage("Multiple exceptions")
	            .hasNoSuppressedExceptions();
	}

	@Test
	public void multipleWithOneVararg() {
		IOException e1 = new IOException("boom");

		assertThat(Exceptions.multiple(e1))
				.isInstanceOf(RuntimeException.class)
				.isExactlyInstanceOf(CompositeException.class)
				.hasMessage("Multiple exceptions")
	            .hasSuppressedException(e1);
	}

	@Test
	public void multipleWithTwoVararg() {
		IOException e1 = new IOException("boom");
		IllegalArgumentException  e2 = new IllegalArgumentException("boom");

		assertThat(Exceptions.multiple(e1, e2))
				.isInstanceOf(RuntimeException.class)
				.isExactlyInstanceOf(CompositeException.class)
				.hasMessage("Multiple exceptions")
	            .hasSuppressedException(e1)
	            .hasSuppressedException(e2);
	}

	@Test
	public void multipleWithNullIterable() {
		//noinspection ConstantConditions
		assertThat(Exceptions.multiple((Iterable<Throwable>) null))
				.isInstanceOf(RuntimeException.class)
				.isExactlyInstanceOf(CompositeException.class)
				.hasMessage("Multiple exceptions")
	            .hasNoSuppressedExceptions();
	}

	@Test
	public void multipleWithEmptyIterable() {
		assertThat(Exceptions.multiple(Collections.emptyList()))
				.isInstanceOf(RuntimeException.class)
				.isExactlyInstanceOf(CompositeException.class)
				.hasMessage("Multiple exceptions")
	            .hasNoSuppressedExceptions();
	}

	@Test
	public void multipleWithIterable() {
		IOException e1 = new IOException("boom");
		IllegalArgumentException  e2 = new IllegalArgumentException("boom");

		assertThat(Exceptions.multiple(Arrays.asList(e1, e2)))
				.isInstanceOf(RuntimeException.class)
				.isExactlyInstanceOf(CompositeException.class)
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

	@Test
	public void addSuppressedToNormal() {
		Exception original = new Exception("foo");
		Exception suppressed = new IllegalStateException("boom");

		assertThat(Exceptions.addSuppressed(original, suppressed))
				.isSameAs(original)
				.hasSuppressedException(suppressed);
	}

	@Test
	public void addSuppressedToRejectedInstance() {
		Throwable original = new RejectedExecutionException("foo");
		Exception suppressed = new IllegalStateException("boom");

		assertThat(Exceptions.addSuppressed(original, suppressed))
				.isSameAs(original)
				.hasSuppressedException(suppressed);
	}

	@Test
	public void addSuppressedToSame() {
		Throwable original = new Exception("foo");

		assertThat(Exceptions.addSuppressed(original, original))
				.isSameAs(original)
				.hasNoSuppressedExceptions();
	}

	@Test
	public void addSuppressedToRejectedSingleton1() {
		Throwable original = REJECTED_EXECUTION;
		Exception suppressed = new IllegalStateException("boom");

		assertThat(Exceptions.addSuppressed(original, suppressed))
				.isNotSameAs(original)
				.hasMessage(original.getMessage())
				.hasSuppressedException(suppressed);
	}

	@Test
	public void addSuppressedToRejectedSingleton2() {
		Throwable original = NOT_TIME_CAPABLE_REJECTED_EXECUTION;
		Exception suppressed = new IllegalStateException("boom");

		assertThat(Exceptions.addSuppressed(original, suppressed))
				.isNotSameAs(original)
				.hasMessage(original.getMessage())
				.hasSuppressedException(suppressed);
	}

	@Test
	public void addSuppressedToTERMINATED() {
		Exception suppressed = new IllegalStateException("boom");

		assertThat(Exceptions.addSuppressed(TERMINATED, suppressed))
				.hasNoSuppressedExceptions()
				.isSameAs(TERMINATED);
	}

	@Test
	public void addSuppressedRuntimeToNormal() {
		RuntimeException original = new RuntimeException("foo");
		Exception suppressed = new IllegalStateException("boom");

		assertThat(Exceptions.addSuppressed(original, suppressed))
				.isSameAs(original)
				.hasSuppressedException(suppressed);
	}

	@Test
	public void addSuppressedRuntimeToRejectedInstance() {
		RuntimeException original = new RejectedExecutionException("foo");
		Exception suppressed = new IllegalStateException("boom");

		assertThat(Exceptions.addSuppressed(original, suppressed))
				.isSameAs(original)
				.hasSuppressedException(suppressed);
	}

	@Test
	public void addSuppressedRuntimeToSame() {
		RuntimeException original = new RuntimeException("foo");

		assertThat(Exceptions.addSuppressed(original, original))
				.isSameAs(original)
				.hasNoSuppressedExceptions();
	}

	@Test
	public void addSuppressedRuntimeToRejectedSingleton1() {
		RuntimeException original = REJECTED_EXECUTION;
		Exception suppressed = new IllegalStateException("boom");

		assertThat(Exceptions.addSuppressed(original, suppressed))
				.isNotSameAs(original)
				.hasMessage(original.getMessage())
				.hasSuppressedException(suppressed);
	}

	@Test
	public void addSuppressedRuntimeToRejectedSingleton2() {
		RuntimeException original = NOT_TIME_CAPABLE_REJECTED_EXECUTION;
		Exception suppressed = new IllegalStateException("boom");

		assertThat(Exceptions.addSuppressed(original, suppressed))
				.isNotSameAs(original)
				.hasMessage(original.getMessage())
				.hasSuppressedException(suppressed);
	}

	@Test
	public void failWithRejectedNormalWraps() {
		Throwable test = new IllegalStateException("boom");
		assertThat(Exceptions.failWithRejected(test))
				.isInstanceOf(Exceptions.ReactorRejectedExecutionException.class)
				.hasMessage("Scheduler unavailable")
				.hasCause(test);
	}

	@Test
	public void failWithRejectedSingletonREEWraps() {
		Throwable test = REJECTED_EXECUTION;
		assertThat(Exceptions.failWithRejected(test))
				.isInstanceOf(Exceptions.ReactorRejectedExecutionException.class)
				.hasMessage("Scheduler unavailable")
				.hasCause(test);
	}

	@Test
	public void failWithRejectedNormalREEWraps() {
		Throwable test = new RejectedExecutionException("boom");
		assertThat(Exceptions.failWithRejected(test))
				.isInstanceOf(Exceptions.ReactorRejectedExecutionException.class)
				.hasMessage("Scheduler unavailable")
				.hasCause(test);
	}

	@Test
	public void failWithRejectedReactorREENoOp() {
		Throwable test = new Exceptions.ReactorRejectedExecutionException("boom", REJECTED_EXECUTION);
		assertThat(Exceptions.failWithRejected(test))
				.isSameAs(test)
				.hasCause(REJECTED_EXECUTION);
	}

	@Test
	public void unwrapMultipleExcludingTraceback() {
		Mono<String> errorMono1 = Mono.error(new IllegalStateException("expected1"));
		Mono<String> errorMono2 = Mono.error(new IllegalStateException("expected2"));
		Mono<Throwable> mono = Mono.zipDelayError(errorMono1, errorMono2)
		                           .checkpoint("checkpoint")
		                           .<Throwable>map(tuple -> new IllegalStateException("should have failed: " + tuple))
		                           .onErrorResume(Mono::just);
		Throwable compositeException = mono.block();

		List<Throwable> exceptions = Exceptions.unwrapMultiple(compositeException);
		List<Throwable> filteredExceptions = Exceptions.unwrapMultipleExcludingTracebacks(compositeException);

		assertThat(exceptions).as("unfiltered composite has traceback")
		                      .hasSize(3);

		assertThat(exceptions).filteredOn(e -> !Exceptions.isTraceback(e))
		                      .as("filtered out tracebacks")
		                      .containsExactlyElementsOf(filteredExceptions)
		                      .hasSize(2)
		                      .hasOnlyElementsOfType(IllegalStateException.class);
	}

	@Test
	public void isRetryExhausted() {
		Throwable match1 = Exceptions.retryExhausted("only a message", null);
		Throwable match2 = Exceptions.retryExhausted("message and cause", new RuntimeException("cause: boom"));
		Throwable noMatch = new IllegalStateException("Retry exhausted: 10/10");

		assertThat(Exceptions.isRetryExhausted(null)).as("null").isFalse();
		assertThat(Exceptions.isRetryExhausted(match1)).as("match1").isTrue();
		assertThat(Exceptions.isRetryExhausted(match2)).as("match2").isTrue();
		assertThat(Exceptions.isRetryExhausted(noMatch)).as("noMatch").isFalse();
	}

	@Test
	public void retryExhaustedMessageWithNoCause() {
		Throwable retryExhausted = Exceptions.retryExhausted("message with no cause", null);

		assertThat(retryExhausted).hasMessage("message with no cause")
		                          .hasNoCause();
	}

	@Test
	public void retryExhaustedMessageWithCause() {
		Throwable retryExhausted = Exceptions.retryExhausted("message with cause", new RuntimeException("boom"));

		assertThat(retryExhausted).hasMessage("message with cause")
		                          .hasCause(new RuntimeException("boom"));
	}
}
