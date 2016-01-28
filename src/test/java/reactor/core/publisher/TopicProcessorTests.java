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

import org.junit.Test;
import org.reactivestreams.Processor;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class TopicProcessorTests extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		return FluxProcessor.blackbox(TopicProcessor.<Long>create("rb-async", bufferSize), Flux::log);
	}

	@Override
	public void required_exerciseWhiteboxHappyPath() throws Throwable {
		super.required_exerciseWhiteboxHappyPath();
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError()
			throws Throwable {
		super.required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError();
	}

	@Test
	public void testShutdown() {
		for (int i = 0; i < 1000; i++) {
			ExecutorProcessor dispatcher = TopicProcessor.create("rb-test-shutdown", 16);
			dispatcher.awaitAndShutdown();
		}
	}

	@Override
	public void required_spec317_mustSupportAPendingElementCountUpToLongMaxValue() throws Throwable {
		super.required_spec317_mustSupportAPendingElementCountUpToLongMaxValue();
	}

	/*@Test
	@Ignore
	public void extra_spec209_mustBePreparedToReceiveAnOnCompleteSignalWithoutPrecedingRequestCall()
			throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.create();
		Publisher<String> publisher = Subscriber::onComplete;
		publisher.subscribe(processor);

		// Waiting till publisher sends Complete into the processor
		Thread.sleep(1000);

		TestSubscriber<String> subscriber = TestSubscriber.createWithTimeoutSecs(1);
		processor.subscribe(subscriber);

		subscriber.assertComplete();
	}*/

}
