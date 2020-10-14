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

package reactor.core.publisher;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxJoinTest {

	final BiFunction<Integer, Integer, Integer> add = (t1, t2) -> t1 + t2;

	<T> Function<Integer, Flux<T>> just(final Flux<T> publisher) {
		return t1 -> publisher;
	}

	@Test
	public void normal1() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		Flux<Integer> m =
				source1.asFlux().join(source2.asFlux(), just(Flux.never()), just(Flux.never()), add);

		m.subscribe(ts);

		source1.emitNext(1, FAIL_FAST);
		source1.emitNext(2, FAIL_FAST);
		source1.emitNext(4, FAIL_FAST);

		source2.emitNext(16, FAIL_FAST);
		source2.emitNext(32, FAIL_FAST);
		source2.emitNext(64, FAIL_FAST);

		source1.emitComplete(FAIL_FAST);
		source2.emitComplete(FAIL_FAST);

		ts.assertValues(17, 18, 20, 33, 34, 36, 65, 66, 68)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normal1WithDuration() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<Integer> duration1 = Sinks.unsafe().many().multicast().directBestEffort();

		Flux<Integer> m = source1.asFlux().join(source2.asFlux(), just(duration1.asFlux()), just(Flux.never()), add);
		m.subscribe(ts);

		source1.emitNext(1, FAIL_FAST);
		source1.emitNext(2, FAIL_FAST);
		source2.emitNext(16, FAIL_FAST);

		duration1.emitNext(1, FAIL_FAST);

		source1.emitNext(4, FAIL_FAST);
		source1.emitNext(8, FAIL_FAST);

		source1.emitComplete(FAIL_FAST);
		source2.emitComplete(FAIL_FAST);

		ts.assertValues(17, 18, 20, 24)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void normal2() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		Flux<Integer> m =
				source1.asFlux().join(source2.asFlux(), just(Flux.never()), just(Flux.never()), add);

		m.subscribe(ts);

		source1.emitNext(1, FAIL_FAST);
		source1.emitNext(2, FAIL_FAST);
		source1.emitComplete(FAIL_FAST);

		source2.emitNext(16, FAIL_FAST);
		source2.emitNext(32, FAIL_FAST);
		source2.emitNext(64, FAIL_FAST);

		source2.emitComplete(FAIL_FAST);

		ts.assertValues(17, 18, 33, 34, 65, 66)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void leftThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		Flux<Integer> m =
				source1.asFlux().join(source2.asFlux(), just(Flux.never()), just(Flux.never()), add);

		m.subscribe(ts);

		source2.emitNext(1, FAIL_FAST);
		source1.emitError(new RuntimeException("Forced failure"), FAIL_FAST);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void rightThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		Flux<Integer> m =
				source1.asFlux().join(source2.asFlux(), just(Flux.never()), just(Flux.never()), add);

		m.subscribe(ts);

		source1.emitNext(1, FAIL_FAST);
		source2.emitError(new RuntimeException("Forced failure"), FAIL_FAST);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void leftDurationThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		Flux<Integer> duration1 = Flux.error(new RuntimeException("Forced failure"));

		Flux<Integer> m = source1.asFlux().join(source2.asFlux(), just(duration1), just(Flux.never()), add);
		m.subscribe(ts);

		source1.emitNext(1, FAIL_FAST);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void rightDurationThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		Flux<Integer> duration1 = Flux.error(new RuntimeException("Forced failure"));

		Flux<Integer> m = source1.asFlux().join(source2.asFlux(), just(Flux.never()), just(duration1), add);
		m.subscribe(ts);

		source2.emitNext(1, FAIL_FAST);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void leftDurationSelectorThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		Function<Integer, Flux<Integer>> fail = t1 -> {
			throw new RuntimeException("Forced failure");
		};

		Flux<Integer> m = source1.asFlux().join(source2.asFlux(), fail, just(Flux.never()), add);
		m.subscribe(ts);

		source1.emitNext(1, FAIL_FAST);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void rightDurationSelectorThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		Function<Integer, Flux<Integer>> fail = t1 -> {
			throw new RuntimeException("Forced failure");
		};

		Flux<Integer> m = source1.asFlux().join(source2.asFlux(), just(Flux.never()), fail, add);
		m.subscribe(ts);

		source2.emitNext(1, FAIL_FAST);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void resultSelectorThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		BiFunction<Integer, Integer, Integer> fail = (t1, t2) -> {
			throw new RuntimeException("Forced failure");
		};

		Flux<Integer> m =
				source1.asFlux().join(source2.asFlux(), just(Flux.never()), just(Flux.never()), fail);
		m.subscribe(ts);

		source1.emitNext(1, FAIL_FAST);
		source2.emitNext(2, FAIL_FAST);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxJoin<Integer, Integer, Integer, Integer, Integer> test = new FluxJoin<>(parent, Flux.just(2), just(Flux.just(3)), just(Flux.just(4)), add);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscription() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, sub -> sub.request(100));
		FluxJoin.JoinSubscription<String, String, String, String, String> test =
				new FluxJoin.JoinSubscription<>(actual,
						s -> Mono.just(s),
						s -> Mono.just(s),
						(l, r) -> l);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		test.request(123);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123);
		test.queue.add(1);
		test.queue.add(5);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.active = 0;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		test.error = new IllegalStateException("boom");
		assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

}
