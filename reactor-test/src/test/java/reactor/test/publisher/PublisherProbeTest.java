/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.test.publisher;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class PublisherProbeTest {

	private static class TestControlFlow {

		private final Mono<Void>   a;
		private final Mono<String> b;

		TestControlFlow(Mono<Void> a, Mono<String> b) {
			this.a = a;
			this.b = b;
		}

		Mono<Void> toTest(Mono<String> monoOfPrincipal) {
			return monoOfPrincipal
					.switchIfEmpty(thisReturnsMonoVoid().then(Mono.empty()))
					.flatMap(this::thisReturnsMonoVoid);
		}

		private Mono<Void> thisReturnsMonoVoid() {
			return this.a;
		}

		private Mono<Void> thisReturnsMonoVoid(String p) {
			return this.b.then();
		}

	}

	@Test
	public void emptyProbeFlowA() {
		PublisherProbe<Void> a = PublisherProbe.empty();
		PublisherProbe<String> b = PublisherProbe.empty();

		TestControlFlow t = new TestControlFlow(a.mono(), b.mono());

		StepVerifier.create(t.toTest(Mono.empty()))
		            .verifyComplete();

		assertThat(a.wasSubscribed()).isTrue();
		assertThat(b.wasSubscribed()).isFalse();

		a.assertWasSubscribed();
		b.assertWasNotSubscribed();
	}

	@Test
	public void emptyProbeFlowB() {
		PublisherProbe<Void> a = PublisherProbe.empty();
		PublisherProbe<String> b = PublisherProbe.empty();

		TestControlFlow t = new TestControlFlow(a.mono(), b.mono());

		StepVerifier.create(t.toTest(Mono.just("principal")))
		            .verifyComplete();

		a.assertWasNotSubscribed();
		b.assertWasSubscribed();

		assertThat(a.wasSubscribed()).isFalse();
		assertThat(b.wasSubscribed()).isTrue();
	}

	@Test
	public void monoProbeFlowA() {
		PublisherProbe<Void> a = PublisherProbe.empty();
		PublisherProbe<String> b = PublisherProbe.of(Mono.just("b"));

		TestControlFlow t = new TestControlFlow(a.mono(), b.mono());

		StepVerifier.create(t.toTest(Mono.empty()))
		            .verifyComplete();

		assertThat(a.wasSubscribed()).isTrue();
		assertThat(b.wasSubscribed()).isFalse();

		a.assertWasSubscribed();
		b.assertWasNotSubscribed();
	}

	@Test
	public void monoProbeFlowB() {
		PublisherProbe<Void> a = PublisherProbe.empty();
		PublisherProbe<String> b = PublisherProbe.of(Mono.just("b"));

		TestControlFlow t = new TestControlFlow(a.mono(), b.mono());

		StepVerifier.create(t.toTest(Mono.just("principal")))
		            .verifyComplete();

		assertThat(a.wasSubscribed()).isFalse();
		assertThat(b.wasSubscribed()).isTrue();

		a.assertWasNotSubscribed();
		b.assertWasSubscribed();
	}

	@Test
	public void wasSubscribedMono() {
		PublisherProbe<Void> probe = PublisherProbe.empty();

		assertThat(probe.wasSubscribed()).isFalse();

		probe.mono().subscribe();

		assertThat(probe.wasSubscribed()).isTrue();
	}

	@Test
	public void wasSubscribedNumberMono() {
		PublisherProbe<Void> probe = PublisherProbe.empty();
		Mono<Void> mono = probe.mono();

		assertThat(probe.subscribeCount()).isEqualTo(0);

		mono.subscribe();
		assertThat(probe.subscribeCount()).isEqualTo(1);

		mono.subscribe();
		assertThat(probe.subscribeCount()).isEqualTo(2);
	}

	@Test
	public void assertWasSubscribedMono() {
		PublisherProbe<Void> probe = PublisherProbe.empty();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasSubscribed)
				.withMessage("PublisherProbe should have been subscribed but it wasn't");

		probe.mono().subscribe();

		probe.assertWasSubscribed();
	}

	@Test
	public void assertWasNotSubscribedMono() {
		PublisherProbe<Void> probe = PublisherProbe.empty();

		probe.assertWasNotSubscribed();

		probe.mono().subscribe();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasNotSubscribed)
				.withMessage("PublisherProbe should not have been subscribed but it was");
	}

	@Test
	public void wasCancelledMono() {
		PublisherProbe<Void> probe = PublisherProbe.of(Mono.never());
		Disposable d = probe.mono().subscribe();

		assertThat(probe.wasCancelled()).isFalse();

		d.dispose();

		assertThat(probe.wasCancelled()).isTrue();
	}

	@Test
	public void assertWasCancelledMono() {
		PublisherProbe<Void> probe = PublisherProbe.of(Mono.never());
		Disposable d = probe.mono().subscribe();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasCancelled)
				.withMessage("PublisherProbe should have been cancelled but it wasn't");

		d.dispose();

		probe.assertWasCancelled();
	}

	@Test
	public void assertWasNotCancelledMono() {
		PublisherProbe<Void> probe = PublisherProbe.of(Mono.never());
		Disposable d = probe.mono().subscribe();

		probe.assertWasNotCancelled();

		d.dispose();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasNotCancelled)
				.withMessage("PublisherProbe should not have been cancelled but it was");
	}

	@Test
	public void wasRequestedMono() {
		PublisherProbe<Void> probe = PublisherProbe.of(Mono.never());
		AtomicReference<Subscription> sub = new AtomicReference<>();
		probe.mono().subscribe(null, null, null, sub::set);

		assertThat(probe.wasRequested()).isFalse();

		sub.get().request(3L);

		assertThat(probe.wasRequested()).isTrue();
	}

	@Test
	public void assertWasRequestedMono() {
		PublisherProbe<Void> probe = PublisherProbe.empty();
		AtomicReference<Subscription> sub = new AtomicReference<>();
		probe.mono().subscribe(null, null, null, sub::set);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasRequested)
				.withMessage("PublisherProbe should have been requested but it wasn't");

		sub.get().request(3L);

		probe.assertWasRequested();
	}

	@Test
	public void assertWasNotRequestedMono() {
		PublisherProbe<Void> probe = PublisherProbe.empty();
		AtomicReference<Subscription> sub = new AtomicReference<>();
		probe.mono().subscribe(null, null, null, sub::set);

		probe.assertWasNotRequested();

		sub.get().request(3L);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasNotRequested)
				.withMessage("PublisherProbe should not have been requested but it was");
	}

	@Test
	public void wasSubscribedFlux() {
		PublisherProbe<Void> probe = PublisherProbe.empty();

		assertThat(probe.wasSubscribed()).isFalse();

		probe.flux().subscribe();

		assertThat(probe.wasSubscribed()).isTrue();
	}

	@Test
	public void wasSubscribedNumberFlux() {
		PublisherProbe<Void> probe = PublisherProbe.empty();
		Flux<Void> mono = probe.flux();

		assertThat(probe.subscribeCount()).isEqualTo(0);

		mono.subscribe();
		assertThat(probe.subscribeCount()).isEqualTo(1);

		mono.subscribe();
		assertThat(probe.subscribeCount()).isEqualTo(2);
	}

	@Test
	public void assertWasSubscribedFlux() {
		PublisherProbe<Void> probe = PublisherProbe.empty();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasSubscribed)
				.withMessage("PublisherProbe should have been subscribed but it wasn't");

		probe.flux().subscribe();

		probe.assertWasSubscribed();
	}

	@Test
	public void assertWasNotSubscribedFlux() {
		PublisherProbe<Void> probe = PublisherProbe.empty();

		probe.assertWasNotSubscribed();

		probe.flux().subscribe();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasNotSubscribed)
				.withMessage("PublisherProbe should not have been subscribed but it was");
	}

	@Test
	public void wasCancelledFlux() {
		PublisherProbe<Void> probe = PublisherProbe.of(Flux.never());
		Disposable d = probe.flux().subscribe();

		assertThat(probe.wasCancelled()).isFalse();

		d.dispose();

		assertThat(probe.wasCancelled()).isTrue();
	}

	@Test
	public void assertWasCancelledFlux() {
		PublisherProbe<Void> probe = PublisherProbe.of(Flux.never());
		Disposable d = probe.flux().subscribe();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasCancelled)
				.withMessage("PublisherProbe should have been cancelled but it wasn't");

		d.dispose();

		probe.assertWasCancelled();
	}

	@Test
	public void assertWasNotCancelledFlux() {
		PublisherProbe<Void> probe = PublisherProbe.of(Flux.never());
		Disposable d = probe.flux().subscribe();

		probe.assertWasNotCancelled();

		d.dispose();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasNotCancelled)
				.withMessage("PublisherProbe should not have been cancelled but it was");
	}

	@Test
	public void wasRequestedFlux() {
		PublisherProbe<Void> probe = PublisherProbe.of(Flux.never());
		AtomicReference<Subscription> sub = new AtomicReference<>();
		probe.flux().subscribeWith(subscriptionCaptorVia(sub));

		assertThat(probe.wasRequested()).isFalse();

		sub.get().request(3L);

		assertThat(probe.wasRequested()).isTrue();
	}

	@Test
	public void assertWasRequestedFlux() {
		PublisherProbe<Void> probe = PublisherProbe.empty();
		AtomicReference<Subscription> sub = new AtomicReference<>();
		probe.flux().subscribeWith(subscriptionCaptorVia(sub));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasRequested)
				.withMessage("PublisherProbe should have been requested but it wasn't");

		sub.get().request(3L);

		probe.assertWasRequested();
	}

	@Test
	public void assertWasNotRequestedFlux() {
		PublisherProbe<Void> probe = PublisherProbe.empty();
		AtomicReference<Subscription> sub = new AtomicReference<>();
		probe.flux().subscribeWith(subscriptionCaptorVia(sub));

		probe.assertWasNotRequested();

		sub.get().request(3L);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(probe::assertWasNotRequested)
				.withMessage("PublisherProbe should not have been requested but it was");
	}

	private <T> Subscriber<T> subscriptionCaptorVia(AtomicReference<Subscription> ref) {
		return new BaseSubscriber<T>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				ref.set(subscription);
			}
		};
	}
}
