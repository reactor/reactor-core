/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test.subscriber;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.THROWABLE;

/**
 * @author Simon Baslé
 */
class DefaultConditionalTestSubscriberTest {

	@Test
	void tryOnNextPassesAddsToList() {
		ConditionalTestSubscriber<Integer> testSubscriber = TestSubscriber.builder().buildConditional(v -> true);

		testSubscriber.tryOnNext(2);

		assertThat(testSubscriber.getReceivedOnNext()).containsExactly(2);
	}

	@Test
	void tryOnNextPassesPostCancellationAddsToBothLists() {
		ConditionalTestSubscriber<Integer> testSubscriber = TestSubscriber.builder().buildConditional(v -> true);

		testSubscriber.tryOnNext(1);
		testSubscriber.cancel();
		testSubscriber.tryOnNext(2);

		assertThat(testSubscriber.getReceivedOnNext()).containsExactly(1, 2);
		assertThat(testSubscriber.getReceivedOnNextAfterCancellation()).containsExactly(2);
		assertThat(testSubscriber.isCancelled()).as("isCancelled").isTrue();
	}

	@Test
	void tryOnNextNotPassingMakesItDisappear() {
		ConditionalTestSubscriber<Integer> testSubscriber = TestSubscriber.builder().buildConditional(v -> false);

		testSubscriber.tryOnNext(1);
		testSubscriber.tryOnNext(2);

		assertThat(testSubscriber.getReceivedOnNext()).as("onNext").isEmpty();
		assertThat(testSubscriber.getReceivedOnNextAfterCancellation()).as("onNextAfterCancellation").isEmpty();
		assertThat(testSubscriber.getProtocolErrors()).as("protocolErrors").isEmpty();
	}

	@Test
	void tryOnNextNotPassingForwardsToDiscardHandler() {
		final List<Object> discarded = new CopyOnWriteArrayList<>();
		ConditionalTestSubscriber<Integer> testSubscriber = TestSubscriber.builder()
				.contextPutAll(Operators.enableOnDiscard(Context.empty(), discarded::add))
				.buildConditional(v -> false);

		testSubscriber.tryOnNext(1);
		testSubscriber.tryOnNext(2);

		assertThat(testSubscriber.getReceivedOnNext()).as("onNext").isEmpty();
		assertThat(testSubscriber.getReceivedOnNextAfterCancellation()).as("onNextAfterCancellation").isEmpty();
		assertThat(testSubscriber.getProtocolErrors()).as("protocolErrors").isEmpty();

		assertThat(discarded).as("discarded list").containsExactly(1, 2);
	}

	@Test
	void tryOnNextWhenTerminatedAddsToProtocolErrors() {
		ConditionalTestSubscriber<Integer> testSubscriber = TestSubscriber.builder()
				.buildConditional(v -> true);

		testSubscriber.tryOnNext(1);
		testSubscriber.onComplete();
		testSubscriber.tryOnNext(2);

		assertThat(testSubscriber.getReceivedOnNext())
				.as("onNext")
				.containsExactly(1);
		assertThat(testSubscriber.getProtocolErrors())
				.as("protocolErrors")
				.containsExactly(Signal.next(2));
	}

	@Test
	void tryOnNextPredicateThatThrowsTriggersOnError() {
		ConditionalTestSubscriber<Integer> testSubscriber = TestSubscriber.builder()
				.buildConditional(v -> {
					if (v == 1) return true;
					throw new IllegalStateException("boom");
				});

		testSubscriber.tryOnNext(1);
		testSubscriber.tryOnNext(2);
		testSubscriber.onError(new IllegalStateException("error after tryOnNext"));

		assertThat(testSubscriber.getReceivedOnNext())
				.as("onNext")
				.containsExactly(1);
		assertThat(testSubscriber.isTerminatedError()).as("isTerminatedError").isTrue();
		assertThat(testSubscriber.expectTerminalError()).as("expectTerminalError")
				.hasMessage("boom")
				.isInstanceOf(IllegalStateException.class);
		assertThat(testSubscriber.getProtocolErrors())
				.as("protocolErrors")
				.extracting(Signal::getThrowable)
				.singleElement(as(THROWABLE))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("error after tryOnNext");
	}

}