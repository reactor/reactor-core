/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.*;

class VoidProcessorTest {

	@Test
	void currentSubscriberCount() {
		Sinks.Empty<Integer> sink = new VoidProcessor<>();

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asMono().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();

		sink.asMono().subscribe();

		assertThat(sink.currentSubscriberCount()).isEqualTo(2);
	}

	@Test
	void resultNotAvailable() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		assertThatIllegalStateException().isThrownBy(() -> mp.block(Duration.ofMillis(1)))
		                                 .withMessageStartingWith("Timeout");
	}

	@SuppressWarnings("deprecation")
	@Test
	void rejectedDoOnSuccessOrError() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnSuccessOrError((s, f) -> ref.set(f)).subscribe(v -> {}, e -> {});
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	void rejectedDoOnTerminate() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet).subscribe(v -> {}, e -> {});
		mp.onError(new Exception("test"));

		assertThat(invoked.get()).isEqualTo(1);
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	void rejectedSubscribeCallback() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.subscribe(v -> {}, ref::set);
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}


	@Test
	void rejectedDoOnError() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(ref::set).subscribe(v -> {}, e -> {});
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	void rejectedSubscribeCallbackNull() {
		VoidProcessor<Void> mp = new VoidProcessor<>();

		assertThatNullPointerException().isThrownBy(() -> mp.subscribe((Subscriber<Void>)null));
	}


	@Test
	void successChainTogether() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		VoidProcessor<Void> mp2 = new VoidProcessor<>();
		mp.subscribe(mp2);

		mp.onComplete();

		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	void rejectedChainTogether() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		VoidProcessor<Void> mp2 = new VoidProcessor<>();
		mp.subscribe(mp2);

		mp.onError(new Exception("test"));

		assertThat(mp2.getError()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	void cancel() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		StepVerifier.create(mp)
					.thenCancel()
					.verify();

		assertThat(mp.downstreamCount()).isEqualTo(0);
	}

	@Test
	void nullFulfill() {
		VoidProcessor<Void> mp = new VoidProcessor<>();

		mp.onNext(null);

		assertThat(mp.isTerminated()).isTrue();
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.peek()).isNull();
	}

	@Test
	void doubleError() {
		VoidProcessor<Void> mp = new VoidProcessor<>();

		mp.onError(new Exception("test"));
		assertThat(mp.tryEmitError(new Exception("test")).hasFailed()).isTrue();
	}

	@Test
	void doubleSignal() {
		VoidProcessor<Void> mp = new VoidProcessor<>();

		mp.onComplete();
		assertThat(mp.tryEmitError(new Exception("test")).hasFailed()).isTrue();
	}

	@Test
	void scanTerminated() {
		Sinks.Empty<Integer> sinkTerminated = new VoidProcessor<>();

		assertThat(sinkTerminated.scan(Scannable.Attr.TERMINATED)).as("not yet terminated").isFalse();

		sinkTerminated.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(sinkTerminated.scan(Scannable.Attr.TERMINATED)).as("terminated with error").isTrue();
		assertThat(sinkTerminated.scan(Scannable.Attr.ERROR)).as("error").hasMessage("boom");
	}

	@Test
	void scanCancelled() {
		Sinks.Empty<Integer> sinkCancelled = new VoidProcessor<>();

		assertThat(sinkCancelled.scan(Scannable.Attr.CANCELLED)).as("pre-cancellation").isFalse();

		((VoidProcessor<?>) sinkCancelled).dispose();

		assertThat(sinkCancelled.scan(Scannable.Attr.CANCELLED)).as("cancelled").isTrue();
	}

	@Test
	void inners() {
		Sinks.Empty<Integer> sink = new VoidProcessor<>();
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink.inners()).as("before subscriptions").isEmpty();

		sink.asMono().subscribe(notScannable);
		sink.asMono().subscribe(scannable);

		assertThat(sink.inners())
				.asList()
				.as("after subscriptions")
				.hasSize(2)
				.extracting(l -> (Object) ((VoidProcessor.VoidInner<?>) l).actual)
				.containsExactly(notScannable, scannable);
	}

}