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

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxGroupJoinTest {

	final BiFunction<Integer, Flux<Integer>, Flux<Integer>> add2 =
			(t1, t2s) -> t2s.map(t2 -> t1 + t2);

	Function<Integer, Flux<Integer>> just(final Flux<Integer> publisher) {
		return t1 -> publisher;
	}

	<T, R> Function<T, Flux<R>> just2(final Flux<R> publisher) {
		return t1 -> publisher;
	}

	@Test
	public void behaveAsJoin() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		Flux<Integer> m =
				source1.groupJoin(source2, just(Flux.never()), just(Flux.never()), add2)
				       .flatMap(t -> t);

		m.subscribe(ts);

		source1.onNext(1);
		source1.onNext(2);
		source1.onNext(4);

		source2.onNext(16);
		source2.onNext(32);
		source2.onNext(64);

		source1.onComplete();
		source2.onComplete();

		ts.assertValues(17, 18, 20, 33, 34, 36, 65, 66, 68)
		  .assertComplete()
		  .assertNoError();
	}

	class Person {

		final int    id;
		final String name;

		public Person(int id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	class PersonFruit {

		final int    personId;
		final String fruit;

		public PersonFruit(int personId, String fruit) {
			this.personId = personId;
			this.fruit = fruit;
		}
	}

	class PPF {

		final Person            person;
		final Flux<PersonFruit> fruits;

		public PPF(Person person, Flux<PersonFruit> fruits) {
			this.person = person;
			this.fruits = fruits;
		}
	}

	@Test
	public void normal1() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux<Person> source1 = Flux.fromIterable(Arrays.asList(new Person(1, "Joe"),
				new Person(2, "Mike"),
				new Person(3, "Charlie")));

		Flux<PersonFruit> source2 =
				Flux.fromIterable(Arrays.asList(new PersonFruit(1, "Strawberry"),
						new PersonFruit(1, "Apple"),
						new PersonFruit(3, "Peach")));

		Mono<PPF> q = source1.groupJoin(source2,
				just2(Flux.never()),
				just2(Flux.never()),
				PPF::new)
		                     .doOnNext(ppf -> ppf.fruits.filter(t1 -> ppf.person.id == t1.personId)
		                                                .subscribe(t1 -> ts.onNext(Arrays.asList(
				                                                ppf.person.name,
				                                                t1.fruit))))
		                     .ignoreElements();

		q.subscribe(ts);
		ts.assertValues(Arrays.asList("Joe", "Strawberry"),
				Arrays.asList("Joe", "Apple"),
				Arrays.asList("Charlie", "Peach"))
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void leftThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		Flux<Flux<Integer>> m =
				source1.groupJoin(source2, just(Flux.never()), just(Flux.never()), add2);

		m.subscribe(ts);

		source2.onNext(1);
		source1.onError(new RuntimeException("Forced failure"));

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void rightThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		Flux<Flux<Integer>> m =
				source1.groupJoin(source2, just(Flux.never()), just(Flux.never()), add2);

		m.subscribe(ts);

		source1.onNext(1);
		source2.onError(new RuntimeException("Forced failure"));

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertValueCount(1);
	}

	@Test
	public void leftDurationThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		Flux<Integer> duration1 = Flux.error(new RuntimeException("Forced failure"));

		Flux<Flux<Integer>> m =
				source1.groupJoin(source2, just(duration1), just(Flux.never()), add2);
		m.subscribe(ts);

		source1.onNext(1);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void rightDurationThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		Flux<Integer> duration1 = Flux.error(new RuntimeException("Forced failure"));

		Flux<Flux<Integer>> m =
				source1.groupJoin(source2, just(Flux.never()), just(duration1), add2);
		m.subscribe(ts);

		source2.onNext(1);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void leftDurationSelectorThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		Function<Integer, Flux<Integer>> fail = t1 -> {
			throw new RuntimeException("Forced failure");
		};

		Flux<Flux<Integer>> m =
				source1.groupJoin(source2, fail, just(Flux.never()), add2);
		m.subscribe(ts);

		source1.onNext(1);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void rightDurationSelectorThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		Function<Integer, Flux<Integer>> fail = t1 -> {
			throw new RuntimeException("Forced failure");
		};

		Flux<Flux<Integer>> m =
				source1.groupJoin(source2, just(Flux.never()), fail, add2);
		m.subscribe(ts);

		source2.onNext(1);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void resultSelectorThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		BiFunction<Integer, Flux<Integer>, Integer> fail = (t1, t2) -> {
			throw new RuntimeException("Forced failure");
		};

		Flux<Integer> m =
				source1.groupJoin(source2, just(Flux.never()), just(Flux.never()), fail);
		m.subscribe(ts);

		source1.onNext(1);
		source2.onNext(2);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void scanGroupJoinSubscription() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, sub -> sub.request(100));
		FluxGroupJoin.GroupJoinSubscription<String, String, String, String, String> test =
				new FluxGroupJoin.GroupJoinSubscription<String, String, String, String, String>(actual,
						s -> Mono.just(s),
						s -> Mono.just(s),
						(l, r) -> l,
						QueueSupplier.unbounded().get(),
						QueueSupplier.<String>one());

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
		test.request(123);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123);
		test.queue.add(5);
		test.queue.add(10);
		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

		test.error = new IllegalArgumentException("boom");
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isSameAs(test.error);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.active = 0;
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	@Test
	public void scanLeftRightSubscriber() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null,
				sub -> sub.request(100));
		FluxGroupJoin.GroupJoinSubscription<String, String, String, String, String> parent =
				new FluxGroupJoin.GroupJoinSubscription<String, String, String, String, String>(actual,
						s -> Mono.just(s),
						s -> Mono.just(s),
						(l, r) -> l,
						QueueSupplier.unbounded().get(),
						QueueSupplier.<String>one());
		FluxGroupJoin.LeftRightSubscriber test = new FluxGroupJoin.LeftRightSubscriber(parent, true);
		Subscription sub = Operators.emptySubscription();
		test.onSubscribe(sub);

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.dispose();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	@Test
	public void scanLeftRightEndSubscriber() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null,
				sub -> sub.request(100));
		FluxGroupJoin.GroupJoinSubscription<String, String, String, String, String> parent =
				new FluxGroupJoin.GroupJoinSubscription<String, String, String, String, String>(actual,
						s -> Mono.just(s),
						s -> Mono.just(s),
						(l, r) -> l,
						QueueSupplier.unbounded().get(),
						QueueSupplier.<String>one());
		FluxGroupJoin.LeftRightEndSubscriber test = new FluxGroupJoin.LeftRightEndSubscriber(parent, false, 1);
		Subscription sub = Operators.emptySubscription();
		test.onSubscribe(sub);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.dispose();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}
}
