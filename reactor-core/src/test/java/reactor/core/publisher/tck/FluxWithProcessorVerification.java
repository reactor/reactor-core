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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import org.reactivestreams.Processor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.TopicProcessor;
import reactor.core.publisher.WorkQueueProcessor;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test //FIXME
public class FluxWithProcessorVerification extends AbstractProcessorVerification {

	final AtomicLong cumulated = new AtomicLong(0);

	final AtomicLong cumulatedJoin = new AtomicLong(0);

	@Override
	public Processor<Long, Long> createIdentityProcessor(int bufferSize) {
		Flux<String> otherStream = Flux.just("test", "test2", "test3");
		System.out.println("Providing new downstream");
		FluxProcessor<Long, Long> p =
				WorkQueueProcessor.<Long>builder().name("fluxion-raw-fork").bufferSize(bufferSize).build();

		cumulated.set(0);
		cumulatedJoin.set(0);

		BiFunction<Long, String, Long> combinator = (t1, t2) -> t1;
		return FluxProcessor.wrap(p,
				p.groupBy(k -> k % 2 == 0)
				 .flatMap(stream -> stream.scan((prev, next) -> next)
				                          .map(integer -> -integer)
				                          .filter(integer -> integer <= 0)
				                          .map(integer -> -integer)
				                          .bufferTimeout(1024, Duration.ofMillis(50))
				                          .flatMap(Flux::fromIterable)
				                          .doOnNext(array -> cumulated.getAndIncrement())
				                          .flatMap(i -> Flux.zip(Flux.just(i),
						                          otherStream,
						                          combinator)))
				 .doOnNext(array -> cumulatedJoin.getAndIncrement())
				 .subscribeWith(TopicProcessor.<Long>builder().name("fluxion-raw-join").bufferSize(bufferSize).build())
				 .doOnError(Throwable::printStackTrace));
	}

	@Override
	public boolean skipStochasticTests() {
		return false;
	}
}
