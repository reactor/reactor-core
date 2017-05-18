/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import org.junit.Test;

import reactor.core.Disposable;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

import static org.junit.Assert.assertEquals;

public class UnicastProcessorTest {

    @Test
    public void secondSubscriberRejectedProperly() {

        UnicastProcessor<Integer> up = UnicastProcessor.Builder.<Integer>create().queue(new ConcurrentLinkedQueue<>()).build();

        up.subscribe();

        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        up.subscribe(ts);

        ts.assertNoValues()
        .assertError(IllegalStateException.class)
        .assertNotComplete();

    }

    @Test
	@Deprecated
	public void factoryMethods() {
		Queue<Integer> overriddenQueue = QueueSupplier.<Integer>unbounded().get();
		Disposable overriddenOnTerminate = () -> {};
		Consumer<? super Integer> overriddenOnOverflow = t -> {};

		UnicastProcessor<Integer> processor = UnicastProcessor.create();
		assertEquals(QueueSupplier.unbounded().get().getClass(), processor.queue.getClass());
		assertEquals(UnicastProcessor.Builder.NOOP_DISPOSABLE, processor.onTerminate);

		processor = UnicastProcessor.create(overriddenQueue);
		assertEquals(overriddenQueue, processor.queue);
		assertEquals(UnicastProcessor.Builder.NOOP_DISPOSABLE, processor.onTerminate);

		processor = UnicastProcessor.create(overriddenQueue, overriddenOnTerminate);
		assertEquals(overriddenQueue, processor.queue);
		assertEquals(overriddenOnTerminate, processor.onTerminate);

		processor = UnicastProcessor.create(overriddenQueue, overriddenOnOverflow, overriddenOnTerminate);
		assertEquals(overriddenQueue, processor.queue);
		assertEquals(overriddenOnOverflow, processor.onOverflow);
		assertEquals(overriddenOnTerminate, processor.onTerminate);
    }
}
