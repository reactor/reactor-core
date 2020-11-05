/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.logging.Level;

import org.reactivestreams.Processor;
import org.testng.SkipException;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class EmitterProcessorVerification extends AbstractProcessorVerification {

	@Override
	@SuppressWarnings("deprecation") // This is ok because this uses FluxProcessor and EmitterProcessor, to be removed in 3.5
	public Processor<Long, Long> createIdentityProcessor(int bufferSize) {
		reactor.core.publisher.FluxProcessor<Long, Long> p = reactor.core.publisher.EmitterProcessor.create(bufferSize);
		return reactor.core.publisher.FluxProcessor.wrap(p, p.log("EmitterProcessorVerification", Level.FINE));
	}

	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo()
			throws Throwable {
		throw new SkipException("WARNING: EmitterProcessor does not emit until all " +
				"subscribers request at least 1");
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError()
			throws Throwable {
		throw new SkipException("WARNING: EmitterProcessor does not emit until all " +
				"subscribers request at least 1");
	}
}
