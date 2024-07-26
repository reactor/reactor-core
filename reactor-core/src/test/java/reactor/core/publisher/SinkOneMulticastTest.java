/*
 * Copyright (c) 2021-2024 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscriber;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.TestLoggerExtension;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.test.StepVerifier;
import reactor.test.subscriber.TestSubscriber;
import reactor.test.util.LoggerUtils;
import reactor.test.util.TestLogger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Simon Baslé
 */
class SinkOneMulticastTest {

	@Test
	void currentSubscriberCount() {
		Sinks.One<Integer> sink = new SinkOneMulticast<>();

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asMono().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();

		sink.asMono().subscribe();

		assertThat(sink.currentSubscriberCount()).isEqualTo(2);
	}

	@Test
	void currentSubscriberCountReflectsCancellation() {
		SinkOneMulticast<Void> mp = new SinkOneMulticast<>();

		//this one tests immediate cancellation, potentially short-circuiting add()
		StepVerifier.create(mp)
			.thenCancel()
			.verify();

		assertThat(mp.currentSubscriberCount()).as("cancelled during add").isZero();

		//this one tests deferred cancellation when the subscriber has 100% been add()ed
		Disposable disposable = mp.subscribe();
		assertThat(mp.currentSubscriberCount()).as("effective add").isOne();

		disposable.dispose();
		assertThat(mp.currentSubscriberCount()).as("disposing effective subscriber").isZero();
	}

	@Test
	void resultNotAvailable() {
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> {
			SinkOneMulticast<String> sink = new SinkOneMulticast<>();
			sink.block(Duration.ofMillis(1));
		});
	}

	@Test
	void rejectedDoOnTerminate() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		AtomicInteger invoked = new AtomicInteger();

		sink.doOnTerminate(invoked::incrementAndGet).subscribe(v -> {}, e -> {});
		EmitResult emitResult = sink.tryEmitError(new Exception("test"));

		assertThat(emitResult).isEqualTo(EmitResult.OK);

		assertThat(invoked).hasValue(1);
		assertThat(sink.scan(Scannable.Attr.ERROR)).hasMessage("test");
		assertThat(sink.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void rejectedSubscribeCallback() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		sink.subscribe(v -> {}, ref::set);
		EmitResult emitResult = sink.tryEmitError(new Exception("test"));

		assertThat(emitResult).isEqualTo(EmitResult.OK);

		assertThat(ref.get()).hasMessage("test");
		assertThat(sink.scan(Scannable.Attr.ERROR)).hasMessage("test");
		assertThat(sink.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void successDoOnTerminate() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		AtomicInteger invoked = new AtomicInteger();

		sink.doOnTerminate(invoked::incrementAndGet).subscribe();
		EmitResult emitResult = sink.tryEmitValue("test");

		assertThat(emitResult).isEqualTo(EmitResult.OK);

		assertThat(invoked).hasValue(1);
		assertThat(sink.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(sink.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void successSubscribeCallback() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		AtomicReference<String> ref = new AtomicReference<>();

		sink.subscribe(ref::set);
		EmitResult emitResult = sink.tryEmitValue("test");

		assertThat(emitResult).isEqualTo(EmitResult.OK);

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(sink.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(sink.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void rejectedDoOnError() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		sink.doOnError(ref::set).subscribe(v -> {}, e -> {});
		EmitResult emitResult = sink.tryEmitError(new Exception("test"));

		assertThat(emitResult).isEqualTo(EmitResult.OK);

		assertThat(ref.get()).hasMessage("test");
		assertThat(sink.scan(Scannable.Attr.ERROR)).hasMessage("test");
		assertThat(sink.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void rejectedSubscribeCallbackNull() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();

		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			sink.subscribe((Subscriber<String>) null);
		});
	}

	@Test
	void successDoOnSuccess() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		AtomicReference<String> ref = new AtomicReference<>();

		sink.doOnSuccess(ref::set).subscribe();
		EmitResult emitResult = sink.tryEmitValue("test");

		assertThat(emitResult).isEqualTo(EmitResult.OK);

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(sink.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(sink.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void doubleFulfill() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();

		StepVerifier.create(sink)
			.then(() -> {
				sink.emitValue("test1", Sinks.EmitFailureHandler.FAIL_FAST);
				sink.emitValue("test2", Sinks.EmitFailureHandler.FAIL_FAST);
			})
			.expectNext("test1")
			.expectComplete()
			.verifyThenAssertThat()
			.hasDroppedExactly("test2");
	}

	@Test
	void nullTryEmitNextIsTurnedToTryEmitComplete() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		TestSubscriber<String> ts = TestSubscriber.create();
		sink.subscribe(ts); //we need to check no onNext(null) is propagated

		EmitResult emitResult = sink.tryEmitValue(null);

		assertThat(emitResult).isEqualTo(EmitResult.OK);

		assertThat(sink.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(sink.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(ts.getReceivedOnNext()).as("received onNext").isEmpty();
		assertThat(ts.isTerminatedComplete());
	}

	@Test
	@TestLoggerExtension.Redirect
	void doubleError(TestLogger testLogger) {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();

		sink.emitError(new Exception("test"), Sinks.EmitFailureHandler.FAIL_FAST);
		sink.emitError(new Exception("test2"), Sinks.EmitFailureHandler.FAIL_FAST);
		Assertions.assertThat(testLogger.getErrContent())
			.contains("Operator called default onErrorDropped")
			.contains("test2");
	}

	@Test
	@TestLoggerExtension.Redirect
	void doubleSignal(TestLogger testLogger) {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();

		sink.emitValue("test", Sinks.EmitFailureHandler.FAIL_FAST);
		sink.emitError(new Exception("test2"), Sinks.EmitFailureHandler.FAIL_FAST);

		Assertions.assertThat(testLogger.getErrContent())
			.contains("Operator called default onErrorDropped")
			.contains("test2");
	}

	@Test
	void blockNegativeIsImmediateTimeout() {
		long start = System.nanoTime();
		SinkOneMulticast<Object> sink = new SinkOneMulticast<>();

		assertThatExceptionOfType(IllegalStateException.class)
			.isThrownBy(() -> sink.block(Duration.ofNanos(-1)))
			.withMessage("Timeout on blocking read for 0 NANOSECONDS")
			.withCause(new TimeoutException("Timeout on blocking read for 0 NANOSECONDS"));

		assertThat(Duration.ofNanos(System.nanoTime() - start))
			.isLessThan(Duration.ofMillis(500));
	}

	@Test
	void blockZeroIsImmediateTimeout() {
		long start = System.nanoTime();
		SinkOneMulticast<Object> sink = new SinkOneMulticast<>();

		assertThatExceptionOfType(IllegalStateException.class)
			.isThrownBy(() -> sink.block(Duration.ZERO))
			.withMessage("Timeout on blocking read for 0 NANOSECONDS")
			.withCause(new TimeoutException("Timeout on blocking read for 0 NANOSECONDS"));

		assertThat(Duration.ofNanos(System.nanoTime() - start))
			.isLessThan(Duration.ofMillis(500));
	}

	@Test
	@Timeout(5)
	void blockNegativeWithWarmedUpSink() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		sink.tryEmitValue("test").orThrow();

		assertThat(sink.block(Duration.ofMillis(-1))).isEqualTo("test");
	}

	@Test
	@Timeout(5)
	void blockZeroWithWarmedUpSink() {
		SinkOneMulticast<String> sink = new SinkOneMulticast<>();
		sink.tryEmitValue("test").orThrow();

		assertThat(sink.block(Duration.ofMillis(0))).isEqualTo("test");
	}

	@Test
	void scanSink() {
		SinkOneMulticast<String> test = new SinkOneMulticast<>();

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		test.tryEmitEmpty().orThrow();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
	}

	@Test
	void scanEmittedError() {
		SinkOneMulticast<Integer> sinkTerminated = new SinkOneMulticast<>();

		assertThat(sinkTerminated.scan(Scannable.Attr.TERMINATED)).as("not yet terminated").isFalse();

		sinkTerminated.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(sinkTerminated.scan(Scannable.Attr.TERMINATED)).as("terminated with error").isTrue();
		assertThat(sinkTerminated.scan(Scannable.Attr.ERROR)).as("error").hasMessage("boom");
	}

	@Test
	void inners() {
		SinkOneMulticast<Integer> sink = new SinkOneMulticast<>();
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink.inners()).as("before subscriptions").isEmpty();

		sink.subscribe(notScannable);
		sink.subscribe(scannable);

		assertThat(sink.inners())
			.asInstanceOf(InstanceOfAssertFactories.LIST)
			.as("after subscriptions")
			.hasSize(2)
			.extracting(l -> (Object) ((SinkOneMulticast.Inner<?>) l).actual())
			.containsExactly(notScannable, scannable);
	}

}