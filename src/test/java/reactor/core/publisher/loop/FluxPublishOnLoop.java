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
package reactor.core.publisher.loop;

import org.junit.Test;
import reactor.core.publisher.FluxPublishOnTest;

/**
 * @author Stephane Maldini
 */
public class FluxPublishOnLoop {

	final FluxPublishOnTest publishOnTest = new FluxPublishOnTest();

	@Test
	public void prefetchAmountOnlyLoop() {
		for (int i = 0; i < 100000; i++) {
			publishOnTest.prefetchAmountOnly();
		}
	}

	@Test
	public void diamondLoop() {
		for (int i = 0; i < 100000; i++) {
			publishOnTest.diamond();
		}
	}

	@Test
	public void boundedQueueLoop() {
		for (int i = 0; i < 1000; i++) {
			publishOnTest.boundedQueue();
		}
	}

	@Test
	public void boundedQueueFilterLoop() {
		for (int i = 0; i < 1000; i++) {
			publishOnTest.boundedQueueFilter();
		}
	}
	@Test
	public void withFlatMapLoop() {
		for (int i = 0; i < 200; i++) {
			publishOnTest.withFlatMap();
		}
	}

	@Test
	public void crossRangeMaxHiddenLoop() throws Exception {
		for (int i = 0; i < 10; i++) {
			publishOnTest.crossRangeMaxHidden();
		}
	}

	@Test
	public void crossRangeMaxLoop() {
		for (int i = 0; i < 50; i++) {
			publishOnTest.crossRangeMax();
		}
	}

	@Test
	public void crossRangeMaxUnboundedLoop() {
		for (int i = 0; i < 50; i++) {
			publishOnTest.crossRangeMaxUnbounded();
		}
	}
}
