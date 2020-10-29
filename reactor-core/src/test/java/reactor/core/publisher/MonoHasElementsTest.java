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

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoHasElementsTest {

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoHasElements<>(null);
		});
	}

	@Test
	public void emptySource() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create();

		Flux.empty().hasElements().subscribe(ts);

		ts.assertValues(false)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void emptyMonoSource() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create();

		Mono.empty().hasElement().subscribe(ts);

		ts.assertValues(false)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void emptySourceBackpressured() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create(0);

		Mono.empty().hasElement().subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(1);

		ts.assertValues(false)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void nonEmptySource() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create();

		Flux.range(1, 10).hasElements().subscribe(ts);

		ts.assertValues(true)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void nonEmptySourceBackpressured() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).hasElements().subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(1);

		ts.assertValues(true)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void fluxSourceIsCancelled() {
		AtomicLong cancelCount = new AtomicLong();

		StepVerifier.create(Flux.range(1, 10)
		                        .doOnCancel(cancelCount::incrementAndGet)
		                        .hasElements())
	                .expectNext(true)
	                .verifyComplete();

		assertThat(cancelCount).hasValue(1);
	}

	@Test
	public void monoSourceIsNotCancelled() {
		AtomicLong cancelCount = new AtomicLong();

		StepVerifier.create(Mono.just(1)
		                        .doOnCancel(cancelCount::incrementAndGet)
		                        .hasElement())
		            .expectNext(true)
		            .verifyComplete();

		assertThat(cancelCount).hasValue(0);
	}

	@Test
	public void testHasElementUpstream() {
		AtomicReference<Subscription> sub = new AtomicReference<>();

		Mono.just("foo").hide()
		    .hasElement()
		    .subscribeWith(new LambdaSubscriber<>(v -> {}, e -> {}, () -> {},
				    s -> {
					    sub.set(s);
					    s.request(Long.MAX_VALUE);
				    }));

		assertThat(sub.get()).isInstanceOf(MonoHasElement.HasElementSubscriber.class);
		assertThat(Scannable.from(sub.get()).scan(Scannable.Attr.PARENT).getClass()).isEqualTo(FluxHide.HideSubscriber.class);
	}

	@Test
	public void testHasElementsUpstream() {
		AtomicReference<Subscription> sub = new AtomicReference<>();

		Flux.just("foo", "bar").hide()
		    .hasElements()
		    .subscribeWith(new LambdaSubscriber<>(v -> {}, e -> {}, () -> {},
				    s -> {
					    sub.set(s);
					    s.request(Long.MAX_VALUE);
				    }));

		assertThat(sub.get()).isInstanceOf(MonoHasElements.HasElementsSubscriber.class);
		Scannable.from(sub.get())
		         .parents()
		         .findFirst()
		         .ifPresent(s -> assertThat(s).isInstanceOf(FluxHide.HideSubscriber
				         .class));
	}

	@Test
	public void hasElementCancel() {
		AtomicBoolean cancelled = new AtomicBoolean();

		Mono.just("foo").hide()
		    .doOnCancel(() -> cancelled.set(true))
		    .log()
		    .hasElement()
		    .subscribeWith(new LambdaSubscriber<>(v -> {}, e -> {}, () -> {},
				    Subscription::cancel));

		assertThat(cancelled.get()).isTrue();
	}

	@Test
	public void hasElementsCancel() {
		AtomicBoolean cancelled = new AtomicBoolean();

		Flux.just("foo", "bar").hide()
		    .doOnCancel(() -> cancelled.set(true))
		    .hasElements()
		    .subscribeWith(new LambdaSubscriber<>(v -> {}, e -> {}, () -> {},
				    Subscription::cancel));

		assertThat(cancelled.get()).isTrue();
	}

	@Test
	public void scanOperatorHasElement(){
		MonoHasElement<Integer> test = new MonoHasElement<>(Mono.just(1));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanOperatorHasElements(){
		MonoHasElements<Integer> test = new MonoHasElements<>(Flux.just(1, 2, 3));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanHasElements() {
		CoreSubscriber<? super Boolean> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoHasElements.HasElementsSubscriber<String> test = new MonoHasElements.HasElementsSubscriber<>(actual);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.onComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
	}

	@Test
	public void scanHasElementsNoTerminatedOnError() {
		CoreSubscriber<? super Boolean> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoHasElements.HasElementsSubscriber<String> test = new MonoHasElements.HasElementsSubscriber<>(actual);

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
	}

	@Test
	public void scanHasElementsCancelled() {
		CoreSubscriber<? super Boolean> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoHasElements.HasElementsSubscriber<String> test = new MonoHasElements.HasElementsSubscriber<>(actual);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanHasElement() {
		CoreSubscriber<? super Boolean> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoHasElement.HasElementSubscriber<String> test = new MonoHasElement.HasElementSubscriber<>(actual);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.onComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
	}

	@Test
	public void scanHasElementNoTerminatedOnError() {
		CoreSubscriber<? super Boolean> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoHasElement.HasElementSubscriber<String> test = new MonoHasElement.HasElementSubscriber<>(actual);

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
	}

	@Test
	public void scanHasElementCancelled() {
		CoreSubscriber<? super Boolean> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoHasElement.HasElementSubscriber<String> test = new MonoHasElement.HasElementSubscriber<>(actual);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

}
