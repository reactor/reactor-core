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
package reactor.core.publisher.scenarios;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.util.Loggers;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

import static java.util.concurrent.TimeUnit.SECONDS;

import reactor.util.Logger;

/**
 * @author Stephane Maldini
 */
public class PopularTagTests extends AbstractReactorTest {

	private static final Logger LOG = Loggers.getLogger(PopularTagTests.class);

	private static final List<String> PULP_SAMPLE = Arrays.asList(
	  "Look, ", "just because I don't be givin' no man a #foot massage don't make it right for #Marsellus #to throw " +
		"Antwone",
	  " ",
	  "into a glass #motherfucker house, ", "fuckin' up the way the nigger talks. ", "#Motherfucker do that shit #to" +
		" " +
		"me,", " he "
	  , "better paralyze my ass, ", "'cause I'll kill the #motherfucker , ", "know what I'm sayin'?"
	);


	@Test
	public void sampleTest() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);

		Disposable top10every1second =
		  Flux.fromIterable(PULP_SAMPLE)
		         .publishOn(asyncGroup)
		         .flatMap(samuelJackson ->
				Flux
				  .fromArray(samuelJackson.split(" "))
				  .publishOn(asyncGroup)
				  .filter(w -> !w.trim().isEmpty())
				  .doOnNext(i -> simulateLatency())
			)
		         .window(Duration.ofSeconds(2))
		         .flatMap(s -> s.groupBy(w -> w)
		                       .flatMap(w -> w.count().map(c -> Tuples.of(w.key(), c)))
		                       .collectSortedList((a, b) -> -a.getT2().compareTo(b.getT2()))
		                        .flatMapMany(Flux::fromIterable)
		                       .take(10)
		                       .doAfterTerminate(() -> LOG.info("------------------------ window terminated" +
						      "----------------------"))
			)
		         .subscribe(
			  entry -> LOG.info(entry.getT1() + ": " + entry.getT2()),
			  error -> LOG.error("", error),
				        latch::countDown
			);

		awaitLatch(top10every1second, latch);
	}

	private void simulateLatency() {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void awaitLatch(Disposable tail, CountDownLatch latch) throws Exception {
		if (!latch.await(10, SECONDS)) {
			throw new Exception("Never completed: (" + latch.getCount() + ")");
		}
	}
}
