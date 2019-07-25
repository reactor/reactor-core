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

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.TestEnvironment;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractFluxVerification
		extends org.reactivestreams.tck.PublisherVerification<Integer> {


	private final Map<Thread, AtomicLong> counters = new ConcurrentHashMap<>();

	final int batch = 1024;

	AbstractFluxVerification() {
		super(new TestEnvironment(false));
	}

	abstract Flux<Integer> transformFlux(Flux<Integer> f);

	@Override
	public Publisher<Integer> createPublisher(long elements) {
		if (elements <= Integer.MAX_VALUE) {
			return Flux.range(1, (int) elements)
			           .filter(integer -> true)
			           .map(integer -> integer)
			           .transform(this::transformFlux);
		}
		else {
			final Random random = new Random();

			return Mono.fromCallable(random::nextInt)
			           .repeat()
			           .map(Math::abs)
			           .transform(this::transformFlux);
		}
	}

	@Override
	public Publisher<Integer> createFailedPublisher() {
		return Flux.<Integer>error(new Exception("boom"))
				.transform(this::transformFlux);
	}

	protected void monitorThreadUse(Object val) {
		AtomicLong counter = counters.get(Thread.currentThread());
		if (counter == null) {
			counter = new AtomicLong();
			counters.put(Thread.currentThread(), counter);
		}
		counter.incrementAndGet();
	}

}
