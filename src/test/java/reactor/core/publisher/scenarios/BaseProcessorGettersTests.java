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
package reactor.core.publisher.scenarios;

import org.junit.Test;
import reactor.core.publisher.EventLoopProcessor;
import reactor.core.publisher.TopicProcessor;
import reactor.core.publisher.WorkQueueProcessor;

import static org.junit.Assert.assertEquals;

/**
 * @author Sergey Shcherbakov
 * @author Stephane Maldini
 */
public class BaseProcessorGettersTests {

	@Test
	public void testTopicProcessorGetters() {

		final int TEST_BUFFER_SIZE = 16;
		EventLoopProcessor<Object, Object> processor = TopicProcessor.create("testProcessor", TEST_BUFFER_SIZE);

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.awaitAndShutdown();

	}

	@Test
	public void testWorkQueueProcessorGetters() {

		final int TEST_BUFFER_SIZE = 16;
		EventLoopProcessor<Object, Object> processor = WorkQueueProcessor.create("testProcessor", TEST_BUFFER_SIZE);

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.onNext(new Object());

		assertEquals(TEST_BUFFER_SIZE - 1, processor.getAvailableCapacity());
		processor.awaitAndShutdown();

	}

}
