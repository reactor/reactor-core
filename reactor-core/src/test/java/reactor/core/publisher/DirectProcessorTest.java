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

import org.junit.jupiter.api.Test;
import java.time.Duration;

import org.reactivestreams.Subscriber;

import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

// This is ok as this class tests the deprecated DirectProcessor. Will be removed with it in 3.5.
@SuppressWarnings("deprecation")
public class DirectProcessorTest {

	@Test
	public void onNextNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			DirectProcessor.create().onNext(null);
		});
	}

	@Test
	public void onErrorNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			DirectProcessor.create().onError(null);
		});
	}

	@Test
	public void onSubscribeNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			DirectProcessor.create().onSubscribe(null);
		});
	}

	@Test
	public void subscribeNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			DirectProcessor.create().subscribe((Subscriber<Object>) null);
		});
	}

	@Test
	public void normal() {
		DirectProcessor<Integer> tp = DirectProcessor.create();

	    StepVerifier.create(tp)
	                .then(() -> {
		                assertThat(tp.hasDownstreams()).as("has downstreams").isTrue();
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

	    assertThat(tp.hasDownstreams()).as("has downstreams").isFalse();
	    assertThat(tp.hasCompleted()).as("hasCompleted").isTrue();
	    assertThat(tp.getError()).as("getError").isNull();
	    assertThat(tp.hasError()).as("hasError").isFalse();
    }

    @Test
    public void normalBackpressured() {
        DirectProcessor<Integer> tp = DirectProcessor.create();

	    StepVerifier.create(tp, 0L)
	                .then(() -> {
		                assertThat(tp.hasDownstreams()).as("has downstreams").isTrue();
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

	    assertThat(tp.hasDownstreams()).as("has downstreams").isFalse();
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

	    assertThat(tp.hasDownstreams()).as("has downstreams").isTrue();
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

	    assertThat(tp.hasDownstreams()).as("has downstreams").isFalse();
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

	    assertThat(tp.hasDownstreams()).as("has downstreams").isFalse();
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

	    assertThat(tp.hasDownstreams()).as("has downstreams").isFalse();
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

	    assertThat(tp.hasDownstreams()).as("has downstreams").isFalse();

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

	    assertThat(tp.hasDownstreams()).as("has downstreams").isTrue();

        tp.onNext(1);

        ts.assertValues(1)
          .assertNoError()
          .assertNotComplete();

        ts.cancel();

	    assertThat(tp.hasDownstreams()).as("has downstreams").isFalse();

        tp.onNext(2);

        ts.assertValues(1)
          .assertNotComplete()
          .assertNoError();
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
	public void onNextWithNoSubscriberJustDiscardsWithoutTerminatingTheSink() {
		DirectProcessor<Integer> directProcessor = DirectProcessor.create();
		directProcessor.onNext(1);

		StepVerifier.create(directProcessor)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .then(() -> directProcessor.onNext(2))
		            .then(directProcessor::onComplete)
		            .expectNext(2)
		            .expectComplete()
		            .verify(Duration.ofSeconds(5));
	}

}
