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
package reactor.core.publisher.tck;

import org.reactivestreams.Processor;
import org.testng.SkipException;
import reactor.core.publisher.TopicProcessor;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class TopicProcessorVerification extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		return TopicProcessor.<Long>builder().name("rb-async").bufferSize(bufferSize).build();
	}

	@Override
	public void required_exerciseWhiteboxHappyPath() throws Throwable {
		super.required_exerciseWhiteboxHappyPath();
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError()
			throws Throwable {
		new SkipException("ci");
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

		AssertSubscriber<String> subscriber = AssertSubscriber.generateTimeoutSecs(1);
		processor.subscribe(subscriber);

		subscriber.assertComplete();
	}*/

}
