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
import reactor.test.TestSubscriber;

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
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

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
		TestSubscriber<Object> ts = TestSubscriber.create();

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
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

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
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

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
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

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
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

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
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

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
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

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
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

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
}
