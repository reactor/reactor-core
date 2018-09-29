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

import org.junit.Test;
import reactor.core.Disposable;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class TransmitProcessorTest {

    @Test
    public void secondSubscriberRejectedProperly() {

        TransmitProcessor<Integer> up = TransmitProcessor.create();

        up.subscribe();

        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        up.subscribe(ts);

        ts.assertNoValues()
        .assertError(IllegalStateException.class)
        .assertNotComplete();

    }

	@Test
	public void createDefault() {
		TransmitProcessor<Integer> processor = TransmitProcessor.create();
		assertProcessor(processor, null);
	}

	public void assertProcessor(TransmitProcessor<Integer> processor,
		@Nullable Disposable onTerminate
	) {
		Disposable expectedOnTerminate = onTerminate;
		assertEquals(expectedOnTerminate, processor.onTerminate);
	}

	@Test
	public void contextTest() {
    	TransmitProcessor<Integer> p = TransmitProcessor.create();
    	p.subscriberContext(ctx -> ctx.put("foo", "bar")).subscribe();

    	assertThat(p.sink().currentContext().get("foo").toString()).isEqualTo("bar");
	}

	@Test
	public void subscriptionCancelNullifiesActual() {
		TransmitProcessor<String> processor = TransmitProcessor.create();

		assertThat(processor.downstreamCount())
				.as("before subscribe")
				.isZero();

		LambdaSubscriber<String> subscriber = new LambdaSubscriber<>(null, null, null, null);
		Disposable subscription = processor.subscribeWith(subscriber);

		assertThat(processor.downstreamCount())
				.as("after subscribe")
				.isEqualTo(1);
		assertThat(processor.actual())
				.as("after subscribe")
				.isSameAs(subscriber);

		subscription.dispose();

		assertThat(processor.downstreamCount())
				.as("after subscription cancel")
				.isZero();
		assertThat(processor.actual())
				.as("after subscription cancel")
				.isNull();
	}
}
