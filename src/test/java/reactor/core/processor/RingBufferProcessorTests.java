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
package reactor.core.processor;

import org.junit.Test;
import org.reactivestreams.Processor;
import reactor.Processors;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class RingBufferProcessorTests extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		return Processors.<Long>topic("rb-async", bufferSize);
	}

	@Test
	public void testShutdown() {
		for (int i = 0; i < 1000; i++) {
			ExecutorProcessor dispatcher = Processors.topic("rb-test-shutdown", 16);
			dispatcher.awaitAndShutdown();
		}
	}

}
