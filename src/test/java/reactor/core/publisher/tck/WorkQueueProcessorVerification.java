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
import reactor.core.publisher.WorkQueueProcessor;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class WorkQueueProcessorVerification extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		System.out.println("new processor");
		return  WorkQueueProcessor.create("rb-work", bufferSize);
	}

	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo()
			throws Throwable {
		//IGNORE since subscribers see distinct data
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError()
			throws Throwable {
		super.required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError();
	}

	@Override
	public void mustImmediatelyPassOnOnErrorEventsReceivedFromItsUpstreamToItsDownstream() throws Exception {
		super.mustImmediatelyPassOnOnErrorEventsReceivedFromItsUpstreamToItsDownstream();
	}

}
