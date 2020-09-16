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

import org.junit.Test;
import org.reactivestreams.Subscriber;

import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class VoidProcessorTest {

	@Test
	public void currentSubscriberCount() {
		Sinks.Empty<Integer> sink = new VoidProcessor<>();

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asMono().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();

		sink.asMono().subscribe();

		assertThat(sink.currentSubscriberCount()).isEqualTo(2);
	}

	@Test(expected = IllegalStateException.class)
	public void NextProcessorResultNotAvailable() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		mp.block(Duration.ofMillis(1));
	}

	@SuppressWarnings("deprecation")
	@Test
	public void VoidProcessorRejectedDoOnSuccessOrError() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnSuccessOrError((s, f) -> ref.set(f)).subscribe();
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void VoidProcessorRejectedDoOnTerminate() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet).subscribe();
		mp.onError(new Exception("test"));

		assertThat(invoked.get()).isEqualTo(1);
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void VoidProcessorRejectedSubscribeCallback() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.subscribe(v -> {}, ref::set);
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}


	@Test
	public void VoidProcessorRejectedDoOnError() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(ref::set).subscribe();
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test(expected = NullPointerException.class)
	public void VoidProcessorRejectedSubscribeCallbackNull() {
		VoidProcessor<Void> mp = new VoidProcessor<>();

		mp.subscribe((Subscriber<Void>)null);
	}


	@Test
	public void VoidProcessorSuccessChainTogether() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		VoidProcessor<Void> mp2 = new VoidProcessor<>();
		mp.subscribe(mp2);

		mp.onComplete();

		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void VoidProcessorRejectedChainTogether() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		VoidProcessor<Void> mp2 = new VoidProcessor<>();
		mp.subscribe(mp2);

		mp.onError(new Exception("test"));

		assertThat(mp2.getError()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void VoidProcessorCancel() {
		VoidProcessor<Void> mp = new VoidProcessor<>();
		StepVerifier.create(mp)
					.thenCancel()
					.verify();

		assertThat(mp.downstreamCount()).isEqualTo(0);
	}

	@Test
	public void VoidProcessorNullFulfill() {
		VoidProcessor<Void> mp = new VoidProcessor<>();

		mp.onNext(null);

		assertThat(mp.isTerminated()).isTrue();
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.peek()).isNull();
	}

	@Test
	public void VoidProcessorDoubleError() {
		VoidProcessor<Void> mp = new VoidProcessor<>();

		mp.onError(new Exception("test"));
		assertThat(mp.tryEmitError(new Exception("test")).hasFailed()).isTrue();
	}

	@Test
	public void VoidProcessorDoubleSignal() {
		VoidProcessor<Void> mp = new VoidProcessor<>();

		mp.onComplete();
		assertThat(mp.tryEmitError(new Exception("test")).hasFailed()).isTrue();
	}

}