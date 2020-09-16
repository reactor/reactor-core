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

import java.time.Duration;

import org.junit.Test;
import org.reactivestreams.Subscriber;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@SuppressWarnings("deprecation")
public class DirectProcessorTest {

    @Test(expected = NullPointerException.class)
    public void onNextNull() {
	    DirectProcessor.create().onNext(null);
    }

    @Test(expected = NullPointerException.class)
    public void onErrorNull() {
	    DirectProcessor.create().onError(null);
    }

    @Test(expected = NullPointerException.class)
    public void onSubscribeNull() {
        DirectProcessor.create().onSubscribe(null);
    }

    @Test(expected = NullPointerException.class)
    public void subscribeNull() {
	    DirectProcessor.create().subscribe((Subscriber<Object>)null);
    }

    @Test
    public void normal() {
        DirectProcessor<Integer> tp = DirectProcessor.create();

	    StepVerifier.create(tp)
	                .then(() -> {
		                assertThat(tp.currentSubscriberCount()).as("has subscriber").isPositive();
		                assertThat(tp.hasCompleted()).as("hasCompleted").isFalse();
		                assertThat(tp.getError()).as("getError").isNull();
		                assertThat(tp.hasError()).as("hasError").isFalse();
	                })
	                .then(() -> {
		                tp.onNext(1);
		                tp.onNext(2);
	                })
	                .expectNext(1, 2)
	                .then(() -> {
		                tp.onNext(3);
		                tp.onComplete();
	                })
	                .expectNext(3)
	                .expectComplete()
	                .verify();

	    assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
	    assertThat(tp.hasCompleted()).as("hasCompleted").isTrue();
	    assertThat(tp.getError()).as("getError").isNull();
	    assertThat(tp.hasError()).as("hasError").isFalse();
    }

    @Test
    public void normalBackpressured() {
        DirectProcessor<Integer> tp = DirectProcessor.create();

	    StepVerifier.create(tp, 0L)
	                .then(() -> {
		                assertThat(tp.currentSubscriberCount()).as("has subscriber").isPositive();
		                assertThat(tp.hasCompleted()).as("hasCompleted").isFalse();
		                assertThat(tp.getError()).as("getError").isNull();
		                assertThat(tp.hasError()).as("hasError").isFalse();
	                })
	                .thenRequest(10L)
	                .then(() -> {
		                tp.onNext(1);
		                tp.onNext(2);
		                tp.onComplete();
	                })
	                .expectNext(1, 2)
	                .expectComplete()
	                .verify();

	    assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
	    assertThat(tp.hasCompleted()).as("hasCompleted").isTrue();
	    assertThat(tp.getError()).as("getError").isNull();
	    assertThat(tp.hasError()).as("hasError").isFalse();
    }

    @Test
    public void notEnoughRequests() {
        DirectProcessor<Integer> tp = DirectProcessor.create();

	    StepVerifier.create(tp, 1L)
	                .then(() -> {
		                tp.onNext(1);
		                tp.onNext(2);
		                tp.onComplete();
	                })
	                .expectNext(1)
	                .expectError(IllegalStateException.class)
	                .verify();
    }

    @Test
    public void error() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        DirectProcessor<Integer> tp = DirectProcessor.create();

        tp.subscribe(ts);

	    assertThat(tp.currentSubscriberCount()).as("has subscriber").isPositive();
	    assertThat(tp.hasCompleted()).as("hasCompleted").isFalse();
	    assertThat(tp.getError()).as("getError").isNull();
	    assertThat(tp.hasError()).as("hasError").isFalse();

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        tp.onNext(1);
        tp.onNext(2);

        ts.assertValues(1, 2)
          .assertNotComplete()
          .assertNoError();

        tp.onNext(3);
        tp.onError(new RuntimeException("forced failure"));

	    assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
	    assertThat(tp.hasCompleted()).as("hasCompleted").isFalse();
	    assertThat(tp.hasError()).as("hasError").isTrue();
	    assertThat(tp.getError()).as("getError")
	                             .isNotNull()
	                             .isExactlyInstanceOf(RuntimeException.class)
	                             .hasMessage("forced failure");

        ts.assertValues(1, 2, 3)
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void terminatedWithError() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        DirectProcessor<Integer> tp = DirectProcessor.create();
        tp.onError(new RuntimeException("forced failure"));

        tp.subscribe(ts);

	    assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
	    assertThat(tp.hasCompleted()).as("hasCompleted").isFalse();
	    assertThat(tp.hasError()).as("hasError").isTrue();
	    assertThat(tp.getError()).as("getError")
	                             .isNotNull()
	                             .isExactlyInstanceOf(RuntimeException.class)
	                             .hasMessage("forced failure");

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void terminatedNormally() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        DirectProcessor<Integer> tp = DirectProcessor.create();
        tp.onComplete();

        tp.subscribe(ts);

	    assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
	    assertThat(tp.hasCompleted()).as("hasCompleted").isTrue();
	    assertThat(tp.getError()).as("getError").isNull();
	    assertThat(tp.hasError()).as("hasError").isFalse();

        ts.assertNoValues()
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void subscriberAlreadyCancelled() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        ts.cancel();

        DirectProcessor<Integer> tp = DirectProcessor.create();

        tp.subscribe(ts);

	    assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();

        tp.onNext(1);


        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();
    }

    @Test
    public void subscriberCancels() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        DirectProcessor<Integer> tp = DirectProcessor.create();

        tp.subscribe(ts);

	    assertThat(tp.currentSubscriberCount()).as("has subscriber").isPositive();

        tp.onNext(1);

        ts.assertValues(1)
          .assertNoError()
          .assertNotComplete();

        ts.cancel();

	    assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();

        tp.onNext(2);

        ts.assertValues(1)
          .assertNotComplete()
          .assertNoError();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void scanInner() {
	    InnerConsumer<? super String> actual = mock(InnerConsumer.class);
        DirectProcessor<String> parent = new DirectProcessor<>();

        DirectProcessor.DirectInner<String> test =
                new DirectProcessor.DirectInner<>(actual, parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    @Test
	public void currentContextDelegatesToFirstSubscriber() {
	    AssertSubscriber<Object> testSubscriber1 = new AssertSubscriber<>(Context.of("key", "value1"));
	    AssertSubscriber<Object> testSubscriber2 = new AssertSubscriber<>(Context.of("key", "value2"));

	    DirectProcessor<Object> directProcessor = new DirectProcessor<>();
	    directProcessor.subscribe(testSubscriber1);
	    directProcessor.subscribe(testSubscriber2);

	    Context processorContext = directProcessor.currentContext();

	    assertThat(processorContext.getOrDefault("key", "EMPTY")).isEqualTo("value1");
    }

	@Test
	public void tryEmitNextWithNoSubscriberFails() {
		DirectProcessor<Integer> directProcessor = DirectProcessor.create();

		assertThat(directProcessor.tryEmitNext(1)).isEqualTo(Sinks.Emission.FAIL_ZERO_SUBSCRIBER);
		assertThat(directProcessor.tryEmitComplete()).isEqualTo(Sinks.Emission.OK);

		StepVerifier.create(directProcessor)
		            .verifyComplete();
	}

	@Test
	public void tryEmitNextWithNoSubscriberFailsIfAllSubscribersCancelled() {
		//in case of autoCancel, removing all subscribers results in TERMINATED rather than EMPTY
		DirectProcessor<Integer> directProcessor = DirectProcessor.create();
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();

		directProcessor.subscribe(testSubscriber);

		assertThat(directProcessor.tryEmitNext(1)).as("emit 1, with subscriber").isEqualTo(Sinks.Emission.OK);
		assertThat(directProcessor.tryEmitNext(2)).as("emit 2, with subscriber").isEqualTo(Sinks.Emission.OK);
		assertThat(directProcessor.tryEmitNext(3)).as("emit 3, with subscriber").isEqualTo(Sinks.Emission.OK);

		testSubscriber.cancel();

		assertThat(directProcessor.tryEmitNext(4)).as("emit 4, without subscriber").isEqualTo(Sinks.Emission.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	public void emitNextWithNoSubscriberJustDiscardsWithoutTerminatingTheSink() {
		DirectProcessor<Integer> directProcessor = DirectProcessor.create();
		directProcessor.emitNext(1);

		StepVerifier.create(directProcessor)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .then(() -> directProcessor.emitNext(2))
		            .then(directProcessor::emitComplete)
		            .expectNext(2)
		            .expectComplete()
		            .verify(Duration.ofSeconds(5));
	}

}
