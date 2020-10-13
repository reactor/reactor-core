/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class TailCallSubscribeTest {

    private static Function<Flux<Object>, Flux<Object>> manyOperatorsOnFlux = flux -> {
        for (int i = 0; i < 5; i++) {
            flux = flux.<Object>map(Object::toString).filter(Objects::nonNull);
        }
        return flux;
    };

    private static Function<Mono<?>, Mono<Object>> manyOperatorsOnMono = mono -> {
        for (int i = 0; i < 5; i++) {
            mono = mono.<Object>map(Object::toString).filter(Objects::nonNull);
        }
        @SuppressWarnings("unchecked")
        Mono<Object> result = (Mono<Object>) mono;
	    return result;
    };

    @Test
    public void testStackDepth() throws Exception {
        StackCapturingPublisher stackCapturingPublisher = new StackCapturingPublisher();
        Mono
                .from(stackCapturingPublisher)
                .as(manyOperatorsOnMono)
                .flux()
                .as(manyOperatorsOnFlux)
                .delayElements(Duration.ofSeconds(1))
                .then()
                .subscribe(new CancellingSubscriber());

        assertThat(stackCapturingPublisher.get(1, TimeUnit.SECONDS))
                .extracting(StackTraceElement::getClassName, StackTraceElement::getMethodName)
                .startsWith(
                        tuple(Thread.class.getName(), "getStackTrace"),
                        tuple(stackCapturingPublisher.getClass().getName(), "subscribe"),
                        tuple(MonoFromPublisher.class.getName(), "subscribe"),
                        tuple(Mono.class.getName(), "subscribe"),
                        tuple(this.getClass().getName(), "testStackDepth")
                );
    }

    @Test
    public void testDebugHook() throws Exception {
        Hooks.onOperatorDebug();
        testStackDepth();
    }

    @Test
    public void interop() throws Exception {
        class CustomOperator implements Publisher<Object> {
            final Flux<Object> flux;

            CustomOperator(Flux<Object> flux) {
                this.flux = flux;
            }

            @Override
            public void subscribe(Subscriber<? super Object> subscriber) {
                flux.subscribe(subscriber);
            }
        }
        StackCapturingPublisher stackCapturingPublisher = new StackCapturingPublisher();
        Mono.from(stackCapturingPublisher)
                .as(manyOperatorsOnMono)
                .flux()
                .as(manyOperatorsOnFlux)
                .transform(flux -> new CustomOperator(flux))
                .as(manyOperatorsOnFlux)
                .then()
                .as(manyOperatorsOnMono)
                .subscribe(new CancellingSubscriber());

        assertThat(stackCapturingPublisher.get(1, TimeUnit.SECONDS))
                .extracting(StackTraceElement::getClassName, StackTraceElement::getMethodName)
                .startsWith(
                        tuple(Thread.class.getName(), "getStackTrace"),
                        tuple(stackCapturingPublisher.getClass().getName(), "subscribe"),
                        tuple(MonoFromPublisher.class.getName(), "subscribe"),
                        tuple(Flux.class.getName(), "subscribe"),
                        tuple(CustomOperator.class.getName(), "subscribe"),
                        tuple(FluxSource.class.getName(), "subscribe"),
                        tuple(Mono.class.getName(), "subscribe"),
                        tuple(this.getClass().getName(), "interop")
                );
    }

	@Test
	public void delaySubscription() throws Exception {
		StackCapturingPublisher stackCapturingPublisher = new StackCapturingPublisher();

		Flux.from(stackCapturingPublisher)
		    .as(manyOperatorsOnFlux)
		    .delaySubscription(Duration.ofMillis(1))
		    .as(manyOperatorsOnFlux)
		    .subscribe(new CancellingSubscriber(Duration.ofMillis(100)));

		assertThat(stackCapturingPublisher.get(1, TimeUnit.SECONDS))
				.extracting(StackTraceElement::getClassName, StackTraceElement::getMethodName)
				.startsWith(
						tuple(Thread.class.getName(), "getStackTrace"),
						tuple(stackCapturingPublisher.getClass().getName(), "subscribe"),
						tuple(FluxSource.class.getName(), "subscribe"),
						tuple(InternalFluxOperator.class.getName(), "subscribe"),
						tuple(FluxDelaySubscription.class.getName(), "accept")
				);
	}

	@Test
	public void doFirst() throws Exception {
		StackCapturingPublisher stackCapturingPublisher = new StackCapturingPublisher();

		Flux.from(stackCapturingPublisher)
		    .as(manyOperatorsOnFlux)
		    .doFirst(() -> {})
		    .as(manyOperatorsOnFlux)
		    .subscribe(new CancellingSubscriber());

		assertThat(stackCapturingPublisher.get(1, TimeUnit.SECONDS))
				.extracting(StackTraceElement::getClassName, StackTraceElement::getMethodName)
				.startsWith(
						tuple(Thread.class.getName(), "getStackTrace"),
						tuple(stackCapturingPublisher.getClass().getName(), "subscribe"),
						tuple(FluxSource.class.getName(), "subscribe"),
						tuple(Flux.class.getName(), "subscribe"),
						tuple(this.getClass().getName(), "doFirst")
				);
	}

	@Test
	public void bufferWhen() throws Exception {
		StackCapturingPublisher stackCapturingPublisher = new StackCapturingPublisher();

		Flux.from(stackCapturingPublisher)
		    .as(manyOperatorsOnFlux)
		    .bufferWhen(Mono.just(1), __ -> Mono.just(2))
		    .cast(Object.class)
		    .as(manyOperatorsOnFlux)
		    .subscribe(new CancellingSubscriber());

		assertThat(stackCapturingPublisher.get(1, TimeUnit.SECONDS))
				.extracting(StackTraceElement::getClassName, StackTraceElement::getMethodName)
				.startsWith(
						tuple(Thread.class.getName(), "getStackTrace"),
						tuple(stackCapturingPublisher.getClass().getName(), "subscribe"),
						tuple(FluxSource.class.getName(), "subscribe"),
						tuple(Flux.class.getName(), "subscribe"),
						tuple(this.getClass().getName(), "bufferWhen")
				);
	}

	@Test
	public void repeat() throws Exception {
		StackCapturingPublisher stackCapturingPublisher = new StackCapturingPublisher();

		Flux.from(stackCapturingPublisher)
		    .as(manyOperatorsOnFlux)
		    .repeat(1)
		    .as(manyOperatorsOnFlux)
		    .subscribe(new CancellingSubscriber(Duration.ofMillis(10)));

		assertThat(stackCapturingPublisher.get(1, TimeUnit.SECONDS))
				.extracting(StackTraceElement::getClassName, StackTraceElement::getMethodName)
				.startsWith(
						tuple(Thread.class.getName(), "getStackTrace"),
						tuple(stackCapturingPublisher.getClass().getName(), "subscribe"),
						tuple(FluxSource.class.getName(), "subscribe"),
						tuple(InternalFluxOperator.class.getName(), "subscribe"),
						tuple(FluxRepeat.RepeatSubscriber.class.getName(), "resubscribe"),
						tuple(FluxRepeat.RepeatSubscriber.class.getName(), "onComplete"),
						tuple(FluxRepeat.class.getName(), "subscribeOrReturn"),
						tuple(Flux.class.getName(), "subscribe"),
						tuple(this.getClass().getName(), "repeat")
				);
	}

	@Test
	public void retry() throws Exception {
		StackCapturingPublisher stackCapturingPublisher = new StackCapturingPublisher();

		Flux.from(stackCapturingPublisher)
		    .as(manyOperatorsOnFlux)
		    .retry(1)
		    .as(manyOperatorsOnFlux)
		    .subscribe(new CancellingSubscriber(Duration.ofMillis(10)));

		assertThat(stackCapturingPublisher.get(1, TimeUnit.SECONDS))
				.extracting(StackTraceElement::getClassName, StackTraceElement::getMethodName)
				.startsWith(
						tuple(Thread.class.getName(), "getStackTrace"),
						tuple(stackCapturingPublisher.getClass().getName(), "subscribe"),
						tuple(FluxSource.class.getName(), "subscribe"),
						tuple(InternalFluxOperator.class.getName(), "subscribe"),
						tuple(FluxRetry.RetrySubscriber.class.getName(), "resubscribe"),
						tuple(FluxRetry.class.getName(), "subscribeOrReturn"),
						tuple(Flux.class.getName(), "subscribe"),
						tuple(this.getClass().getName(), "retry")
				);
	}

    private static class StackCapturingPublisher extends CompletableFuture<StackTraceElement[]> implements Publisher<Object> {

        @Override
        public void subscribe(Subscriber<? super Object> subscriber) {
            new Exception().printStackTrace(System.out);
            complete(Thread.currentThread().getStackTrace());
            subscriber.onSubscribe(Operators.emptySubscription());
            subscriber.onComplete();
        }
    }

    private static class CancellingSubscriber implements Subscriber<Object> {

    	final Duration cancellationDelay;

	    volatile Disposable cancellation = Disposables.disposed();

	    private CancellingSubscriber() {
	    	this(Duration.ZERO);
	    }

	    private CancellingSubscriber(Duration delay) {
		    cancellationDelay = delay;
	    }

	    @Override
        public void onSubscribe(Subscription s) {
	    	if (cancellationDelay.isZero()) {
	    		s.cancel();
		    } else {
			    cancellation = Schedulers.parallel().schedule(s::cancel, cancellationDelay.toMillis(), TimeUnit.MILLISECONDS);
		    }
	    }

        @Override
        public void onNext(Object s) {
        }

        @Override
        public void onError(Throwable throwable) {
            throwable.printStackTrace();
	        cancellation.dispose();
        }

        @Override
        public void onComplete() {
	        cancellation.dispose();
        }
    }
}
