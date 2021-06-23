/*
 * Copyright (c) 2011-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class ParallelMergeOrderedTest {

	// see https://github.com/reactor/reactor-core/issues/1958
	@Test
	void dealsWithPrefetchLargerThanSmallBufferSize() {
		int parallelism = 2;  // parallelism must be > 1 to expose issue
		int bufferS = Queues.SMALL_BUFFER_SIZE;
		int orderedPrefetch = bufferS + 128; // if orderedPrefetch > bufferS then operator used to drop elements and eventually hang

		final AtomicInteger prev = new AtomicInteger(-1);

		Flux.range(0, 200_000)
		    .subscribeOn(Schedulers.newSingle("init", true))
		    .parallel(parallelism, bufferS)
		    .runOn(Schedulers.newParallel("process", parallelism, true), bufferS)
		    .map(i -> i)
		    .ordered(Comparator.comparing(i -> i), orderedPrefetch)
		    .as(StepVerifier::create)
		    .thenConsumeWhile(current -> {
			    int previous = prev.getAndSet(current);
			    try {
				    assertThat(current)
						    .withFailMessage("elements dropped: prev: %d, next: %d, lost: %d\n", previous, current, current - previous)
						    .isEqualTo(previous + 1);
			    }
			    catch (AssertionError ae) {
				    ae.printStackTrace();
			    }
			    return true;
		    })
		    .expectComplete()
		    .verify(Duration.ofSeconds(5)); //should run in 3s
	}

	@Test
	public void reorderingByIndex() {
		final int LOOPS = 100;
		final int PARALLELISM = 2;
		final List<Integer> ordered = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		int notShuffled = 0;
		for (int i = 0; i < LOOPS; i++) {
			final Scheduler SCHEDULER = Schedulers.newParallel("test", PARALLELISM);
			final List<Integer> disordered = Collections.synchronizedList(new ArrayList<>());

			List<Integer> reordered = Flux.fromIterable(ordered)
			                         .hide()
			                         .index()
			                         .parallel(PARALLELISM)
			                         .runOn(SCHEDULER)
			                         .doOnNext(t2 -> disordered.add(t2.getT2()))
			                         .ordered(Comparator.comparing(Tuple2::getT1))
			                         .map(Tuple2::getT2)
			                         .collectList()
			                         .block();

			SCHEDULER.dispose();

			assertThat(reordered).containsExactlyElementsOf(ordered);
			assertThat(disordered).containsExactlyInAnyOrderElementsOf(ordered);

			try {
				assertThat(disordered).doesNotContainSequence(ordered);
				System.out.println("parallel shuffled the collection into " + disordered);
				break;
			}
			catch (AssertionError e) {
				notShuffled++;
			}
		}
		if (notShuffled > 0) {
			System.out.println("not shuffled loops: " + notShuffled);
		}

		assertThat(LOOPS - notShuffled)
				.as("at least one run shuffled")
				.isGreaterThan(0);
	}

	@Test
	public void rejectPrefetchZero() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ParallelMergeOrdered<>(null, 0, null))
				.withMessage("prefetch > 0 required but it was 0");
	}

	@Test
	public void rejectPrefetchNegative() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ParallelMergeOrdered<>(null,-1, null))
				.withMessage("prefetch > 0 required but it was -1");
	}

	@Test
	public void getPrefetch() {
		ParallelMergeOrdered<Integer> test = new ParallelMergeOrdered<>(null, 123, null);

		assertThat(test.getPrefetch()).isEqualTo(123);
	}

	@Test
	public void getPrefetchAPI() {
		Flux<Integer> test = Flux.range(1, 10)
		                         .parallel()
		                         .ordered(Comparator.naturalOrder(), 123);

		assertThat(test.getPrefetch()).isEqualTo(123);
	}

	@Test
	public void scanUnsafe() {
		ParallelFlux<Integer> source = Flux.range(1, 10)
		                                   .parallel(2);
		ParallelMergeOrdered<Integer> test = new ParallelMergeOrdered<>(source, 123, null);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);

		assertThat(test.scan(Scannable.Attr.NAME)).isNull();
	}
}
