/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicBoolean;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

@RunWith(JUnitParamsRunner.class)
public class MonoHandleTest {

	@Test
	public void normal() {
		Mono.just(1)
		    .handle((v, s) -> s.next(v * 2))
		    .subscribeWith(AssertSubscriber.create())
		    .assertContainValues(singleton(2))
		    .assertNoError()
		    .assertComplete();
	}
	@Test
	public void normalHide() {
		Mono.just(1)
		    .hide()
		    .handle((v, s) -> s.next(v * 2))
		    .subscribeWith(AssertSubscriber.create())
		    .assertContainValues(singleton(2))
		    .assertNoError()
		    .assertComplete();
	}

	@Test
	public void filterNullMapResult() {

		Mono.just(1)
		    .handle((v, s) -> { /*ignore*/ })
		    .subscribeWith(AssertSubscriber.create())
		    .assertValueCount(0)
		    .assertNoError()
		    .assertComplete();
	}


	private static final String sourceErrorMessage = "boomSource";
	private static final String sourceData = "foo";

	private Object[] sourcesError() {
		return new Object[] {
				new Object[] { Mono.error(new IllegalStateException(sourceErrorMessage))
						.hide() },
				new Object[] { Mono.error(new IllegalStateException(sourceErrorMessage))
				                   .hide().filter(i -> true) },
				new Object[] { Mono.error(new IllegalStateException(sourceErrorMessage)) },
				new Object[] { Mono.error(new IllegalStateException(sourceErrorMessage))
						.filter(i -> true) }
		};
	}

	private Object[] sourcesComplete() {
		return new Object[] {
				new Object[] { Mono.just(sourceData).hide() },
				new Object[] { Mono.just(sourceData).hide().filter(i -> true) },
				new Object[] { Mono.just(sourceData) },
				new Object[] { Mono.just(sourceData).filter(i -> true) }
		};
	}

	@Test
	@Parameters(method = "sourcesError")
	public void sourceErrorHandleNotInvoked(Mono<String> source) {
		//the source errors so the handler is never invoked
		AtomicBoolean handlerInvoked = new AtomicBoolean();
		Mono<String> test = source.handle((t, sink) -> handlerInvoked.set(true));

		StepVerifier.create(test)
		            .expectErrorMessage(sourceErrorMessage)
		            .verify();

		Assertions.assertThat(handlerInvoked)
		          .as("handler shouldn't be invoked")
		          .isFalse();
	}

	@Test
	@Parameters(method = "sourcesComplete")
	public void sourceCompleteHandleError(Mono<String> source) {
		//the source completes but handle calls sink.error during data event
		RuntimeException sinkException = new IllegalStateException("boom2");
		Mono<String> test = source.handle((t, sink) -> {
			sink.next(t);
			sink.error(sinkException);
		});

		StepVerifier.create(test)
		            .expectNext(sourceData)
		            .expectErrorMessage("boom2")
		            .verify();
	}

	@Test
	@Parameters(method = "sourcesComplete")
	public void sourceCompleteHandleComplete(Mono<String> source) {
		//the source completes but handle calls sink.error during data event
		Mono<String> test = source.handle((t, sink) -> {
			sink.next(t);
			sink.complete();
		});

		StepVerifier.create(test)
		            .expectNext(sourceData)
		            .expectComplete()
		            .verify();
	}

	@Test
	@Parameters(method = "sourcesError")
	public void sourceErrorHandleTerminateComplete(Mono<String> source) {
		//the source errors and no terminal sink method is called during data event, terminate callback calls sink.complete
		Mono<String> test = source.handle((t, sink) -> sink.next(t),
				(opt, sink) -> sink.complete());

		StepVerifier.create(test)
		            .expectComplete()
		            .verify();
	}

	@Test
	@Parameters(method = "sourcesError")
	public void sourceErrorHandleTerminateError(Mono<String> source) {
		//the source errors and no terminal sink method is called during data event, terminate callback calls sink.error
		RuntimeException sinkException = new IllegalStateException("boom2");
		Mono<String> test = source.handle((t, sink) -> sink.next(t),
				(opt, sink) -> sink.error(sinkException));

		StepVerifier.create(test)
		            .expectErrorMessage("boom2")
		            .verify();
	}

	@Test
	@Parameters(method = "sourcesComplete")
	public void sourceCompleteHandleTerminateComplete(Mono<String> source) {
		//the source completes and no terminal sink method is called during data event, terminate callback calls sink.complete
		Mono<String> test = source.handle((t, sink) -> sink.next(t),
				(opt, sink) -> sink.complete());

		StepVerifier.create(test)
		            .expectNext(sourceData)
		            .expectComplete()
		            .verify();
	}

	@Test
	@Parameters(method = "sourcesComplete")
	public void sourceCompleteHandleTerminateError(Mono<String> source) {
		//the source completes and no terminal sink method is called during data event, terminate callback calls sink.error
		RuntimeException sinkException = new IllegalStateException("boom2");
		Mono<String> test = source.handle((t, sink) -> sink.next(t),
				(opt, sink) -> sink.error(sinkException));

		StepVerifier.create(test)
		            .expectNext(sourceData)
		            .expectErrorMessage("boom2")
		            .verify();
	}

	@Test
	@Parameters(method = "sourcesComplete")
	public void sourceCompleteHandleTerminateNoOp(Mono<String> source) {
		StepVerifier.create(source.handle((t, sink) -> sink.next(t),
				(opt, sink) -> {}))
		            .expectNext(sourceData)
		            .verifyComplete();
	}

	@Test
	@Parameters(method = "sourcesError")
	public void sourceErrorHandleTerminateNoOp(Mono<String> source) {
		StepVerifier.create(source.handle((t, sink) -> sink.next(t),
				(opt, sink) -> {}))
		            .verifyErrorMessage(sourceErrorMessage);
	}

	@Test
	@Parameters(method = "sourcesComplete")
	public void sourceCompleteHandleTerminateNextRejected(Mono<String> source) {
		Mono<Object> test = source.handle((t, sink) -> sink.next(t), (opt, sink) -> sink.next("last"));
		assertThatIllegalStateException().isThrownBy(test::subscribe)
		                                 .withMessage("Cannot emit in terminateHandler");
	}

	@Test
	@Parameters(method = "sourcesError")
	public void sourceErrorHandleTerminateNextRejected(Mono<String> source) {
		Mono<Object> test = source.handle((t, sink) -> sink.next(t), (opt, sink) -> sink.next("last"));
		assertThatIllegalStateException().isThrownBy(test::subscribe)
		                                 .withMessage("Cannot emit in terminateHandler");
	}

}
