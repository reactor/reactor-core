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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Sinks;
import reactor.test.subscriber.AssertSubscriber;

import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxWindowConsistencyTest {

	Sinks.Many<Integer> sourceProcessor = Sinks.unsafe().many().multicast().directBestEffort();

	Flux<Integer> source;

	AssertSubscriber<Flux<Integer>> mainSubscriber;

	private AtomicInteger sourceCount = new AtomicInteger();

	private AtomicInteger innerCreated = new AtomicInteger();

	private AtomicInteger innerCancelled = new AtomicInteger();

	private AtomicInteger innerCompleted = new AtomicInteger();

	private AtomicInteger innerTerminated = new AtomicInteger();

	private AtomicInteger mainCancelled = new AtomicInteger();

	private AtomicInteger mainCompleted = new AtomicInteger();

	private AtomicInteger mainTerminated = new AtomicInteger();

	@BeforeEach
	public void setUp() {
		source = sourceProcessor.asFlux().doOnNext(i -> sourceCount.incrementAndGet());
	}

	private void generate(int start, int count) {
		for (int i = 0; i < count; i++) {
			sourceProcessor.emitNext(i + start, FAIL_FAST);
		}
	}

	private void generateWithCancel(int start, int countBeforeCancel, int countAfterCancel) {
		generate(start, countBeforeCancel);
		mainSubscriber.cancel();
		generate(start + countBeforeCancel, countAfterCancel);
	}

	private void generateAndComplete(int start, int count) {
		generate(start, count);
		sourceProcessor.emitComplete(FAIL_FAST);
		generate(start + count, 10);
	}

	private void expectWindow(int index, Predicate<? super Integer> innerCancelPredicate, List<Integer> values) {
		AssertSubscriber<Integer> s = AssertSubscriber.create();
		mainSubscriber.values().get(index)
				.doOnCancel(() -> innerCancelled.incrementAndGet())
				.doOnComplete(() -> {
					innerCompleted.incrementAndGet();})
				.doOnTerminate(() -> innerTerminated.incrementAndGet())
				.takeWhile(innerCancelPredicate).subscribe(s);

		s.assertValueSequence(values).assertNoError();
		innerCreated.incrementAndGet();
	}

	private void subscribe(Flux<Flux<Integer>> windows) {
		mainSubscriber = AssertSubscriber.create();
		windows.doOnCancel(() -> mainCancelled.incrementAndGet())
			   .doOnComplete(() -> mainCompleted.incrementAndGet())
			   .doOnTerminate(() -> mainTerminated.incrementAndGet()).subscribe(mainSubscriber);
	}

	private void subscribeGroups(Flux<GroupedFlux<Integer, Integer>> groups) {
		subscribe(groups.map(m -> m));
	}

	@SafeVarargs
	final void verifyMainComplete(List<Integer>... lists) {
		for (int i = 0; i < lists.length; i++) {
			expectWindow(i, t -> true, lists[i]);
		}
		assertThat(innerCancelled).as("Inner cancel doesn't match").hasValue(0);
		assertThat(innerCompleted).as("Inner complete doesn't match").hasValue(lists.length);
		assertThat(innerTerminated).as("Inner terminate doesn't match").hasValue(lists.length);
		assertThat(mainCancelled).as("Main cancel doesn't match").hasValue(0);
		assertThat(mainCompleted).as("Main complete doesn't match").hasValue(1);
		assertThat(mainTerminated).as("Main terminate doesn't match").hasValue(1);
		assertThat(innerCompleted).as("Inner not completed").hasValue(innerCreated.get());
	}

	@SafeVarargs
	final void verifyMainCancel(boolean isGroupBy, List<Integer>... lists) {
		for (int i = 0; i < lists.length; i++) {
			expectWindow(i, t -> true, lists[i]);
		}

		// All tests except groupBy provide sufficient data/duration for all inner windows to complete
		int expectedInnerComplete = isGroupBy ? lists.length : 0;
		assertThat(innerCancelled).as("Inner cancel doesn't match").hasValue(0);
		assertThat(innerCompleted).as("Inner complete doesn't match").hasValue(expectedInnerComplete);
		assertThat(innerTerminated).as("Inner terminate doesn't match").hasValue(expectedInnerComplete);
		assertThat(mainCancelled).as("Main cancel doesn't match").hasValue(1);
		assertThat(mainCompleted).as("Main complete doesn't match").hasValue(0);
		assertThat(mainTerminated).as("Main terminate doesn't match").hasValue(0);
	}

	@SafeVarargs
	final void verifyMainCancelNoNewWindow(int completedWindows, List<Integer>... lists) {
		for (int i = 0; i < lists.length; i++) {
			expectWindow(i, t -> true, lists[i]);
		}
		assertThat(innerCancelled).as("Inner cancel doesn't match").hasValue(0);
		assertThat(innerCompleted).as("Inner complete doesn't match").hasValue(completedWindows);
		assertThat(innerTerminated).as("Inner terminate doesn't match").hasValue(completedWindows);
		assertThat(mainCancelled).as("Main cancel doesn't match").hasValue(1);
		assertThat(mainCompleted).as("Main complete doesn't match").hasValue(0);
		assertThat(mainTerminated).as("Main terminate doesn't match").hasValue(0);
	}

	@SafeVarargs
	final void verifyInnerCancel(int completedWindows, Predicate<? super Integer> predicate, List<Integer>... lists) {
		for (int i = 0; i < lists.length; i++) {
			expectWindow(i, predicate, lists[i]);
		}

		assertThat(innerCancelled).as("Inner cancel doesn't match").hasValue(lists.length - completedWindows);
		assertThat(innerCompleted).as("Inner complete doesn't match").hasValue(completedWindows);
		assertThat(innerTerminated).as("Inner terminate doesn't match").hasValue(completedWindows);
		assertThat(mainCancelled).as("Main cancel doesn't match").hasValue(1);
		assertThat(mainCompleted).as("Main complete doesn't match").hasValue(0);
		assertThat(mainTerminated).as("Main terminate doesn't match").hasValue(0);
	}

	@Test
	public void windowExactComplete() throws Exception {
		Flux<Flux<Integer>> windows = source.window(3, 3);
		subscribe(windows);
		generateAndComplete(0, 6);
		verifyMainComplete(Arrays.asList(0, 1, 2), Arrays.asList(3, 4, 5));
	}

	@Test
	public void windowSkipComplete() throws Exception {
		Flux<Flux<Integer>> windows = source.window(3, 5);
		subscribe(windows);
		generateAndComplete(0, 10);
		verifyMainComplete(Arrays.asList(0, 1, 2), Arrays.asList(5, 6, 7));
	}

	@Test
	public void windowOverlapComplete() throws Exception {
		Flux<Flux<Integer>> windows = source.window(5, 3);
		subscribe(windows);
		generateAndComplete(0, 5);
		verifyMainComplete(Arrays.asList(0, 1, 2, 3, 4), Arrays.asList(3, 4));
	}

	@Test
	public void windowDurationComplete() throws Exception {
		Flux<Flux<Integer>> windows = source.window(Duration.ofMillis(200));
		subscribe(windows);
		generate(0, 3);
		Thread.sleep(300);
		generateAndComplete(3, 3);
		verifyMainComplete(Arrays.asList(0, 1, 2), Arrays.asList(3, 4, 5));
	}

	@Test
	public void windowTimeoutComplete() throws Exception {
		Flux<Flux<Integer>> windows = source.windowTimeout(5, Duration.ofMillis(200));
		subscribe(windows);
		generate(0, 3);
		Thread.sleep(300);
		generateAndComplete(3, 3);
		verifyMainComplete(Arrays.asList(0, 1, 2), Arrays.asList(3, 4, 5));
	}

	@Test
	public void windowBoundaryComplete() throws Exception {
		Sinks.Many<Integer> boundary = Sinks.unsafe().many().multicast().directBestEffort();
		Flux<Flux<Integer>> windows = source.window(boundary.asFlux());
		subscribe(windows);
		generate(0, 3);
		boundary.emitNext(1, FAIL_FAST);
		generateAndComplete(3, 3);
		verifyMainComplete(Arrays.asList(0, 1, 2), Arrays.asList(3, 4, 5));
	}

	@Test
	public void windowStartEndComplete() throws Exception {
		Sinks.Many<Integer> start = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> end1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> end2 = Sinks.unsafe().many().multicast().directBestEffort();
		Flux<Flux<Integer>> windows = source.windowWhen(start.asFlux(), v -> v == 1 ? end1.asFlux() : end2.asFlux());
		subscribe(windows);
		start.emitNext(1, FAIL_FAST);
		generate(0, 3);
		end1.emitNext(1, FAIL_FAST);
		start.emitNext(2, FAIL_FAST);
		generateAndComplete(3, 3);
		verifyMainComplete(Arrays.asList(0, 1, 2), Arrays.asList(3, 4, 5));
	}

	@Test
	public void windowUntilComplete() throws Exception {
		Flux<Flux<Integer>> windows = source.windowUntil(i -> i % 3 == 0);
		subscribe(windows);
		generateAndComplete(1, 5);
		verifyMainComplete(Arrays.asList(1, 2, 3), Arrays.asList(4, 5));
	}

	@Test
	public void windowWhileComplete() throws Exception {
		Flux<Flux<Integer>> windows = source.windowWhile(i -> i % 3 != 0);
		subscribe(windows);
		generateAndComplete(1, 5);
		verifyMainComplete(Arrays.asList(1, 2), Arrays.asList(4, 5));
	}

	@Test
	public void groupByComplete() throws Exception {
		Flux<GroupedFlux<Integer, Integer>> windows = source.groupBy(i -> i % 2);
		subscribeGroups(windows);
		generateAndComplete(0, 6);
		verifyMainComplete(Arrays.asList(0, 2, 4), Arrays.asList(1, 3, 5));
	}

	@Test
	public void windowExactMainCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.window(5, 5);
		subscribe(windows);
		generateWithCancel(0, 7, 10);
		verifyMainCancel(true, Arrays.asList(0, 1, 2, 3, 4), Arrays.asList(5, 6, 7, 8, 9));
	}

	@Test
	public void windowSkipMainCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.window(3, 5);
		subscribe(windows);
		generateWithCancel(0, 6, 10);
		verifyMainCancel(true, Arrays.asList(0, 1, 2), Arrays.asList(5, 6, 7));
	}

	@Test
	public void windowOverlapMainCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.window(5, 3);
		subscribe(windows);
		generateWithCancel(0, 4, 10);
		verifyMainCancel(true, Arrays.asList(0, 1, 2, 3, 4), Arrays.asList(3, 4, 5, 6, 7));
	}

	@Test
	public void windowDurationMainCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.window(Duration.ofMillis(100));
		subscribe(windows);
		generate(0, 2);
		mainSubscriber.cancel();
		generate(2, 3);
		Thread.sleep(200);
		generate(5, 10);
		verifyMainCancel(true, Arrays.asList(0, 1, 2, 3, 4));
	}

	@Test
	public void windowTimeoutMainCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.windowTimeout(10, Duration.ofMillis(100));
		subscribe(windows);
		generate(0, 2);
		mainSubscriber.cancel();
		generate(2, 3);
		Thread.sleep(200);
		generate(5, 10);
		verifyMainCancel(true);
	}

	@Test
	public void windowBoundaryMainCancel() throws Exception {
		Sinks.Many<Integer> boundary = Sinks.unsafe().many().multicast().directBestEffort();
		Flux<Flux<Integer>> windows = source.window(boundary.asFlux());

		subscribe(windows);
		generate(0, 3);
		boundary.emitNext(1, FAIL_FAST);
		generate(3, 1);
		mainSubscriber.cancel();
		generate(4, 2);
		boundary.emitNext(1, FAIL_FAST);
		generate(6, 10);
		verifyMainCancel(true, Arrays.asList(0, 1, 2), Arrays.asList(3, 4, 5));
	}

	@Test
	public void windowStartEndMainCancel() throws Exception {
		Sinks.Many<Integer> start = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> end1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> end2 = Sinks.unsafe().many().multicast().directBestEffort();
		Flux<Flux<Integer>> windows = source.windowWhen(start.asFlux(), v -> v == 1 ? end1.asFlux() : end2.asFlux());
		subscribe(windows);
		start.emitNext(1, FAIL_FAST);
		generate(0, 3);
		end1.emitNext(1, FAIL_FAST);
		start.emitNext(2, FAIL_FAST);
		generate(3, 1);
		mainSubscriber.cancel();
		generate(4, 2);
		end2.emitNext(1, FAIL_FAST);
		start.emitNext(3, FAIL_FAST);
		generate(7, 10);
		verifyMainCancel(true, Arrays.asList(0, 1, 2), Arrays.asList(3, 4, 5));
	}

	@Test
	public void windowUntilMainCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.windowUntil(i -> i % 3 == 0);
		subscribe(windows);
		generateWithCancel(1, 4, 10);
		verifyMainCancel(true, Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6));
	}

	@Test
	public void windowWhileMainCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.windowWhile(i -> i % 3 != 0);
		subscribe(windows);
		generateWithCancel(1, 4, 10);
		verifyMainCancel(true, Arrays.asList(1, 2), Arrays.asList(4, 5));
	}

	@Test
	public void groupByMainCancel() throws Exception {
		Flux<GroupedFlux<Integer, Integer>> windows = source.groupBy(i -> i % 2);
		subscribeGroups(windows);
		generateWithCancel(0, 5, 1);
		verifyMainCancel(false, Arrays.asList(0, 2, 4), Arrays.asList(1, 3, 5));
	}

	@Test
	public void windowExactMainCancelNoNewWindow() throws Exception {
		Flux<Flux<Integer>> windows = source.window(5, 5);
		subscribe(windows);
		generateWithCancel(0, 10, 1);
		verifyMainCancelNoNewWindow(2, Arrays.asList(0, 1, 2, 3, 4), Arrays.asList(5, 6, 7, 8, 9));
	}

	@Test
	public void windowSkipMainCancelNoNewWindow() throws Exception {
		Flux<Flux<Integer>> windows = source.window(2, 5);
		subscribe(windows);
		generateWithCancel(0, 5, 1);
		verifyMainCancelNoNewWindow(1, Arrays.asList(0, 1));
	}

	@Test
	public void windowOverlapMainCancelNoNewWindow() throws Exception {
		Flux<Flux<Integer>> windows = source.window(5, 1);
		subscribe(windows);
		generateWithCancel(0, 2, 1);
		verifyMainCancelNoNewWindow(0, Arrays.asList(0, 1, 2), Arrays.asList(1, 2));
	}

	@Test
	public void windowDurationMainCancelNoNewWindow() throws Exception {
		Flux<Flux<Integer>> windows = source.window(Duration.ofMillis(100));
		subscribe(windows);
		generate(0, 2);
		mainSubscriber.cancel();
		Thread.sleep(200);
		generate(2, 1);
		verifyMainCancelNoNewWindow(1, Arrays.asList(0, 1));
	}

	@Test
	public void windowTimeoutMainCancelNoNewWindow() throws Exception {
		Flux<Flux<Integer>> windows = source.windowTimeout(5, Duration.ofMillis(200));
		subscribe(windows);
		generate(0, 1);
		Thread.sleep(300);
		generate(1, 1);
		mainSubscriber.cancel();
		Thread.sleep(300);
		generate(2, 1);
		verifyMainCancelNoNewWindow(1, Arrays.asList(0), Arrays.asList(1));
	}

	@Test
	public void windowBoundaryMainCancelNoNewWindow() throws Exception {
		Sinks.Many<Integer> boundary = Sinks.unsafe().many().multicast().directBestEffort();
		Flux<Flux<Integer>> windows = source.window(boundary.asFlux());

		subscribe(windows);
		generate(0, 3);
		boundary.emitNext(1, FAIL_FAST);
		mainSubscriber.cancel();
		generate(3, 1);
		verifyMainCancelNoNewWindow(1, Arrays.asList(0, 1, 2));
	}

	@Test
	public void windowStartEndMainCancelNoNewWindow() throws Exception {
		Sinks.Many<Integer> start = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> end1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> end2 = Sinks.unsafe().many().multicast().directBestEffort();
		Flux<Flux<Integer>> windows = source.windowWhen(start.asFlux(), v -> v == 1 ? end1.asFlux() : end2.asFlux());
		subscribe(windows);
		start.emitNext(1, FAIL_FAST);
		generate(0, 4);
		end1.emitNext(1, FAIL_FAST);
		start.emitNext(2, FAIL_FAST);
		mainSubscriber.cancel();
		generate(5, 1);
		verifyMainCancelNoNewWindow(1, Arrays.asList(0, 1, 2, 3));
	}

	@Test
	public void windowUntilMainCancelNoNewWindow() throws Exception {
		Flux<Flux<Integer>> windows = source.windowUntil(i -> i % 3 == 0);
		subscribe(windows);
		generateWithCancel(0, 4, 1);
		verifyMainCancelNoNewWindow(2, Arrays.asList(0), Arrays.asList(1, 2, 3));
	}

	@Test
	public void windowWhileMainCancelNoNewWindow() throws Exception {
		Flux<Flux<Integer>> windows = source.windowWhile(i -> i % 3 != 1);
		subscribe(windows);
		generateWithCancel(0, 4, 1);
		verifyMainCancelNoNewWindow(2, Arrays.asList(0), Arrays.asList(2, 3));
	}

	@Test
	public void groupByMainCancelNoNewWindow() throws Exception {
		Flux<GroupedFlux<Integer, Integer>> windows = source.groupBy(i -> i % 2);
		subscribeGroups(windows);
		generateWithCancel(0, 1, 1);
		verifyMainCancelNoNewWindow(0, Arrays.asList(0));
	}

	@Test
	public void windowExactInnerCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.window(5, 5);
		subscribe(windows);
		generateWithCancel(0, 49, 1);
		verifyInnerCancel(1, i -> i != 7, Arrays.asList(0, 1, 2, 3, 4), Arrays.asList(5, 6));
	}

	@Test
	public void windowSkipInnerCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.window(2, 5);
		subscribe(windows);
		generateWithCancel(0, 6, 1);
		verifyInnerCancel(1, i -> i != 6, Arrays.asList(0, 1), Arrays.asList(5));
	}

	@Test
	public void windowOverlapInnerCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.window(5, 1);
		subscribe(windows);
		generateWithCancel(0, 6, 1);
		verifyInnerCancel(0, i -> i != 2, Arrays.asList(0, 1), Arrays.asList(1));
	}

	@Test
	public void windowDurationInnerCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.window(Duration.ofMillis(5000));
		subscribe(windows);
		generateWithCancel(0, 6, 1);
		verifyInnerCancel(0, i -> i != 2, Arrays.asList(0, 1));
	}

	@Test
	public void windowTimeoutInnerCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.windowTimeout(5, Duration.ofMillis(5000));
		subscribe(windows);
		generateWithCancel(0, 6, 1);
		verifyInnerCancel(0, i -> i != 2, Arrays.asList(0, 1));
	}

	@Test
	public void windowBoundaryInnerCancel() throws Exception {
		Sinks.Many<Integer> boundaryProcessor = Sinks.unsafe().many().multicast().directBestEffort();
		Flux<Flux<Integer>> windows = source.window(boundaryProcessor.asFlux());
		subscribe(windows);
		generateWithCancel(0, 6, 1);
		verifyInnerCancel(0, i -> i != 2, Arrays.asList(0, 1));
	}

	@Test
	public void windowStartEndInnerCancel() throws Exception {
		Sinks.Many<Integer> start = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> end1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> end2 = Sinks.unsafe().many().multicast().directBestEffort();
		Flux<Flux<Integer>> windows = source.windowWhen(start.asFlux(), v -> v == 1 ? end1.asFlux() : end2.asFlux());
		subscribe(windows);
		start.emitNext(1, FAIL_FAST);
		generateWithCancel(0, 6, 1);
		verifyInnerCancel(0, i -> i != 2, Arrays.asList(0, 1));
	}

	@Test
	public void windowUntilInnerCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.windowUntil(i -> i % 3 == 0);
		subscribe(windows);
		generateWithCancel(0, 6, 1);
		verifyInnerCancel(1, i -> i != 3, Arrays.asList(0), Arrays.asList(1, 2));
	}

	@Test
	public void windowWhileInnerCancel() throws Exception {
		Flux<Flux<Integer>> windows = source.windowWhile(i -> i % 3 != 1);
		subscribe(windows);
		generateWithCancel(0, 6, 1);
		verifyInnerCancel(1, i -> i != 3, Arrays.asList(0), Arrays.asList(2));
	}

	@Test
	public void groupByInnerCancel() throws Exception {
		Flux<GroupedFlux<Integer, Integer>> windows = source.groupBy(i -> i % 2);
		subscribeGroups(windows);
		generateWithCancel(0, 9, 1);
		verifyInnerCancel(0, i -> i < 6, Arrays.asList(0, 2, 4), Arrays.asList(1, 3, 5));
	}
}
