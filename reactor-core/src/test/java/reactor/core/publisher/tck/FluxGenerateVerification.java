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

package reactor.core.publisher.tck;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.SkipException;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

/**
 * @author Stephane Maldini
 */
@Test
public class FluxGenerateVerification extends PublisherVerification<Long> {

	public FluxGenerateVerification() {
		super(new TestEnvironment(500), 1000);
	}

	@Override
	public Publisher<Long> createPublisher(long elements) {
		return Flux.<Long, Long>generate(() -> 0L, (cursor, s) -> {
			if(cursor < elements && cursor < elements) {
				s.next(cursor);
			}
			else if(cursor == elements){
				s.complete();
			}

			return cursor + 1;
		})

				.map(data -> data * 10)
				.map( data -> data / 10)
				.log("log-test");
	}

	@Override
	public Publisher<Long> createFailedPublisher() {
		return Flux.error(new Exception("test"));
	}

	@Override
	public void required_spec309_requestZeroMustSignalIllegalArgumentException()
			throws Throwable {
		throw new SkipException("Reactor doesn't onError on <= 0 request, it throws");
	}

	@Override
	public void required_spec309_requestNegativeNumberMustSignalIllegalArgumentException()
			throws Throwable {
		throw new SkipException("Reactor doesn't onError on <= 0 request, it throws");
	}
}
