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

import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;
import reactor.Flux;

/**
 * @author Stephane Maldini
 */
@Test
public class FluxVerification extends PublisherVerification<Long> {

	public FluxVerification() {
		super(new TestEnvironment(500, true), 1000);
	}

	@org.junit.Test
	public void simpleTest() {

	}

	@Override
	public Publisher<Long> createPublisher(long elements) {
		return FluxFactory.<Long, AtomicLong>create((s) -> {
			long cursor = s.context()
			               .getAndIncrement();
			if (cursor < elements) {
				s.onNext(cursor);
			}
			else {
				s.onComplete();
			}
		}, s -> new AtomicLong(0L)).lift(FluxLift.<Long, Long>lifter((data, sub) -> sub.onNext(data * 10)))
		                           .log("log-test");
	}

	@Override
	public Publisher<Long> createFailedPublisher() {
		return Flux.error(new Exception("test"));
	}
}
