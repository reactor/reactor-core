/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.publisher.tck;

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Processor;
import org.testng.annotations.AfterClass;
import reactor.core.publisher.EmitterProcessor;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class EmitterProcessorVerification extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		return EmitterProcessor.create(bufferSize);
	}

	@Override
	public void required_spec109_subscribeThrowNPEOnNullSubscriber() throws Throwable {
		super.required_spec109_subscribeThrowNPEOnNullSubscriber();
	}

	@Override
	public boolean skipStochasticTests() {
		return true;
	}

	@AfterClass
	@Override
	public void tearDown() throws InterruptedException{
		executorService.shutdown();
		executorService.awaitTermination(1, TimeUnit.SECONDS);
	}

}
