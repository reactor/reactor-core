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

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.*;

class SinkEmptyMulticastTest {

	@Test
	void currentSubscriberCount() {
		Sinks.Empty<Integer> sink = new SinkEmptyMulticast<>();

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asMono().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();

		sink.asMono().subscribe();

		assertThat(sink.currentSubscriberCount()).isEqualTo(2);
	}

	@Test
	void currentSubscriberCountReflectsCancellation() {
		SinkEmptyMulticast<Void> mp = new SinkEmptyMulticast<>();
		StepVerifier.create(mp)
				.thenCancel()
				.verify();

		assertThat(mp.currentSubscriberCount()).isEqualTo(0);
	}

	@Test
	void blockReturnsNullOnTryEmitEmpty() {
		SinkEmptyMulticast<Void> mp = new SinkEmptyMulticast<>();

		Schedulers.parallel().schedule(() -> mp.tryEmitEmpty(), 50L, TimeUnit.MILLISECONDS);
		assertThat(mp.block(Duration.ofSeconds(1))).isNull();
	}

	@Test
	void blockThrowsOnTryEmitError() {
		SinkEmptyMulticast<Void> mp = new SinkEmptyMulticast<>();

		Schedulers.parallel().schedule(() -> mp.tryEmitError(new IllegalStateException("boom")), 50L, TimeUnit.MILLISECONDS);
		assertThatIllegalStateException().isThrownBy(() -> mp.block(Duration.ofSeconds(1))).withMessage("boom");
	}

	@Test
	void blockTimeoutsOnUnusedSink() {
		SinkEmptyMulticast<Void> mp = new SinkEmptyMulticast<>();
		assertThatIllegalStateException().isThrownBy(() -> mp.block(Duration.ofMillis(1)))
				.withMessageStartingWith("Timeout");
	}

	@Test
	void tryEmitEmpty(){
		SinkEmptyMulticast<Void> mp = new SinkEmptyMulticast<>();
		AtomicBoolean completed = new AtomicBoolean();

		mp.subscribe(c ->{}, ec -> {}, () -> completed.set(true));
		mp.tryEmitEmpty().orThrow();

		assertThat(completed).isTrue();
		assertThat(mp.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(mp.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void tryEmitError() {
		SinkEmptyMulticast<Void> mp = new SinkEmptyMulticast<>();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(ref::set).subscribe(v -> {
		}, e -> {
		});
		mp.tryEmitError(new Exception("test")).orThrow();

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.scan(Scannable.Attr.ERROR)).isSameAs(ref.get());
		assertThat(mp.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void cantSubscribeWithNullSubscriber() {
		SinkEmptyMulticast<Void> mp = new SinkEmptyMulticast<>();

		assertThatNullPointerException().isThrownBy(() -> mp.subscribe((Subscriber<Void>) null));
	}

	@Test
	void doubleError() {
		SinkEmptyMulticast<Void> mp = new SinkEmptyMulticast<>();

		mp.tryEmitError(new Exception("test")).orThrow();
		assertThat(mp.tryEmitError(new Exception("test"))).isEqualTo(Sinks.EmitResult.FAIL_TERMINATED);
	}

	@Test
	void doubleSignal() {
		SinkEmptyMulticast<Void> mp = new SinkEmptyMulticast<>();

		mp.tryEmitEmpty().orThrow();
		assertThat(mp.tryEmitError(new Exception("test"))).isEqualTo(Sinks.EmitResult.FAIL_TERMINATED);
	}

	@Test
	void scanOperator() {
		Sinks.Empty<Integer> sinkTerminated = new SinkEmptyMulticast<>();

		assertThat(sinkTerminated.scan(Scannable.Attr.TERMINATED)).as("not yet terminated").isFalse();

		sinkTerminated.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(sinkTerminated.scan(Scannable.Attr.TERMINATED)).as("terminated with error").isTrue();
		assertThat(sinkTerminated.scan(Scannable.Attr.ERROR)).as("error").hasMessage("boom");
	}

	@Test
	void inners() {
		Sinks.Empty<Integer> sink = new SinkEmptyMulticast<>();
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {
		};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink.inners()).as("before subscriptions").isEmpty();

		sink.asMono().subscribe(notScannable);
		sink.asMono().subscribe(scannable);

		assertThat(sink.inners())
				.asList()
				.as("after subscriptions")
				.hasSize(2)
				.extracting(l -> (Object) ((SinkEmptyMulticast.VoidInner<?>) l).actual)
				.containsExactly(notScannable, scannable);
	}

}