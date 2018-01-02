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
import java.util.function.BiFunction;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class FluxBlackboxProcessorVerification extends AbstractFluxVerification {

	private Scheduler sharedGroup;

	@Override
	Flux<Integer> transformFlux(Flux<Integer> f) {
		Flux<String> otherStream = Flux.just("test", "test2", "test3");
//		System.out.println("Providing new downstream");

		Scheduler asyncGroup = Schedulers.newParallel("flux-p-tck", 2);

		BiFunction<Integer, String, Integer> combinator = (t1, t2) -> t1;

		return f.publishOn(sharedGroup)
		        .parallel(2)
		        .groups()
		        .flatMap(stream -> stream.publishOn(asyncGroup)
				                          .doOnNext(this::monitorThreadUse)
				                          .scan((prev, next) -> next)
				                          .map(integer -> -integer)
				                          .filter(integer -> integer <= 0)
				                          .map(integer -> -integer)
				                          .bufferTimeout(batch, Duration.ofMillis(50))
				                          .flatMap(Flux::fromIterable)
				                          .flatMap(i -> Flux.zip(Flux.just(i), otherStream, combinator))
				 )
		        .publishOn(sharedGroup)
		        .doAfterTerminate(asyncGroup::dispose)
		        .doOnError(Throwable::printStackTrace);
	}

	@BeforeMethod
	public void init() {
		sharedGroup = Schedulers.newParallel("fluxion-tck", 2);
	}

	@AfterMethod
	public void tearDown(){
		sharedGroup.dispose();
	}

}
