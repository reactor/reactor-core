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
package reactor.core

import org.reactivestreams.Subscriber
import reactor.core.publisher.EmitterProcessor
import reactor.core.publisher.Mono
import reactor.core.publisher.MonoProcessor
import reactor.core.publisher.SchedulerGroup
import reactor.core.subscriber.DeferredScalarSubscriber
import reactor.core.util.Exceptions
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Stephane Maldini
 */
class MonoSpec extends Specification {

  def "An onComplete consumer is called when a promise is rejected"() {
	given: "a MonoProcessor with an onComplete Consumer"
	def promise = MonoProcessor.create()
	def acceptedMonoProcessor

	promise.doOnTerminate { success, failure -> acceptedMonoProcessor = failure }.subscribeWith(MonoProcessor.create())

	when: "the promise is rejected"
	promise.onError new Exception()

	then: "the consumer is invoked with the promise"
	acceptedMonoProcessor == promise.getError()
  }

  def "An onComplete consumer is called when added to an already-rejected promise"() {
	given: "a rejected MonoProcessor"
	def promise = MonoProcessor.<Object> error(new Exception())

	when: "an onComplete consumer is added"
	def acceptedMonoProcessor

	promise.doOnTerminate { data, failure -> acceptedMonoProcessor = failure }.subscribeWith(MonoProcessor.create())

	then: "the consumer is invoked with the promise"
	acceptedMonoProcessor == promise.getError()
  }

  def "An onComplete consumer is called when a promise is fulfilled"() {
	given: "a MonoProcessor with an onComplete Consumer"
	def promise = MonoProcessor.create()
	def acceptedMonoProcessor

	promise.doOnTerminate() { v, error -> acceptedMonoProcessor = v }.subscribeWith(MonoProcessor.create())

	when: "the promise is fulfilled"
	promise.onNext 'test'

	then: "the consumer is invoked with the promise"
	acceptedMonoProcessor == promise.get()
	promise.success
  }

  def "An onComplete consumer is called when added to an already-fulfilled promise"() {
	given: "a fulfilled MonoProcessor"
	def promise = Mono.just('test')

	when: "an onComplete consumer is added"
	def acceptedMonoProcessor

	promise.doOnTerminate{ self, err -> acceptedMonoProcessor = self}.subscribeWith(MonoProcessor.create())

	then: "the consumer is invoked with the promise"
	acceptedMonoProcessor == promise.get()
  }

  def "An onSuccess consumer is called when a promise is fulfilled"() {
	given: "a MonoProcessor with an doOnSuccess Consumer"
	def promise = MonoProcessor.create()
	def acceptedValue

	promise.doOnSuccess { v -> acceptedValue = v }.subscribeWith(MonoProcessor.create())

	when: "the promise is fulfilled"
	promise.onNext 'test'

	then: "the consumer is invoked with the fulfilling value"
	acceptedValue == 'test'
  }

  def "An onSuccess consumer is called when added to an already-fulfilled promise"() {
	given: "a fulfilled MonoProcessor"
	def promise = Mono.just('test')

	when: "an doOnSuccess consumer is added"
	def acceptedValue

	promise.doOnSuccess { v -> acceptedValue = v }.subscribeWith(MonoProcessor.create())

	then: "the consumer is invoked with the fulfilling value"
	acceptedValue == 'test'
  }

  def "An onSuccess consumer can be added to an already-rejected promise"() {
	given: "a rejected MonoProcessor"
	def promise = Mono.error(new Exception())

	when: "an doOnSuccess consumer is added"
	def ex = null
	promise.doOnError { ex = it }.subscribe()

	then: "no error is thrown"
	thrown Exception
	ex in Exception
  }

  def "An onError consumer can be added to an already-fulfilled promise"() {
	given: "a fulfilled MonoProcessor"
	def promise = Mono.just('test')

	when: "an doOnError consumer is added"
	promise.doOnSuccess {}

	then: "no error is thrown"
	notThrown Exception
  }

  def "An onError consumer is called when a promise is rejected"() {
	given: "a MonoProcessor with an doOnError Consumer"
	def promise = MonoProcessor.create()
	def acceptedValue

	promise.doOnError { v -> acceptedValue = v }.subscribeWith(MonoProcessor.create())

	when: "the promise is rejected"
	def failure = new Exception()
	promise.onError failure

	then: "the consumer is invoked with the rejecting value"
	thrown Exception
  }

  def "A promise can only listen to terminal states"() {
	given: "a MonoProcessor with an doOnError Consumer"
	def promise = MonoProcessor.create()
	def after = MonoProcessor.create()
	promise.after().subscribe(after)

	when: "the promise is fulfilled"
	promise.onNext "test"

	then: "the promise is invoked without the accepted value"
	!after.peek()
	after.isTerminated()
	after.isSuccess()

	when: "the promise is rejected"
	promise = MonoProcessor.create()
	after = MonoProcessor.create()
	promise.after().subscribe(after)

	promise.onError new Exception()

	then: "the promise is invoked with the rejecting value"
	thrown(Exception)
	after.isError()
	after.getError().class == Exception
  }

  def "An onError consumer is called when added to an already-rejected promise"() {
	given: "a rejected MonoProcessor"
	def failure = new Exception()
	def promise = Mono.error(failure)

	when: "an doOnError consumer is added"
	def acceptedValue

	promise.doOnError { v -> acceptedValue = v
	}.subscribeWith(MonoProcessor.create())
	println promise.debug()

	then: "the consumer is invoked with the rejecting value"
	thrown(Exception)
	acceptedValue == failure
  }

  def "When getting a rejected promise's value the exception that the promise was rejected with is thrown"() {
	given: "a rejected MonoProcessor"
	def failure = new Exception()
	def promise = Mono.error(failure)

	when: "getting the promise's value"
	promise.get()

	then: "the error that the promise was rejected with is thrown"
	thrown(Exception)
  }

  def "A fulfilled promise's value is returned by get"() {
	given: "a fulfilled MonoProcessor"
	def promise = Mono.just('test')

	when: "getting the promise's value"
	def value = promise.get()

	then: "the value used to fulfil the promise is returned"
	value == 'test'
  }

  def "A promise can be fulfilled with null"() {
	given: "a promise"
	def promise = MonoProcessor.<Object> create()

	when: "the promise is fulfilled with null"
	promise.onNext null

	then: "the promise has completed"
	promise.isTerminated()
  }

  def "A function can be used to map a MonoProcessor's value when it's fulfilled"() {
	given: "a promise with a mapping function"
	def promise = MonoProcessor.<Integer> create()
	def mappedMonoProcessor = promise.map { it * 2 }

	when: "the original promise is fulfilled"
	promise.onNext 1

	then: "the mapped promise is fulfilled with the mapped value"
	mappedMonoProcessor.get() == 2
  }

  def "A map many can be used to bind to another MonoProcessor and compose asynchronous results "() {
	given: "a promise with a map many function"
	def promise = MonoProcessor.<Integer> create()
	def mappedMonoProcessor = promise.then { Mono.just(it + 1) }

	when: "the original promise is fulfilled"
	println promise.debug()
	promise.onNext 1
	println promise.debug()

	then: "the mapped promise is fulfilled with the mapped value"
	mappedMonoProcessor.get() == 2
  }

  def "A function can be used to map an already-fulfilled MonoProcessor's value"() {
	given: "a fulfilled promise with a mapping function"
	def promise = Mono.just(1)

	when: "a mapping function is added"
	def mappedMonoProcessor = promise.map { it * 2 }

	then: "the mapped promise is fulfilled with the mapped value"
	mappedMonoProcessor.get() == 2
  }

  def "An onSuccess consumer registered via then is called when the promise is fulfilled"() {
	given: "A promise with an doOnSuccess consumer registered using then"
	MonoProcessor<String> promise = MonoProcessor.<String> create()
	def value = null
	promise.doOnSuccess { value = it }.subscribeWith(MonoProcessor.create())

	when: "The promise is fulfilled"
	promise.onNext 'test'

	then: "the consumer is called"
	value == 'test'
  }

  def "An onError consumer registered via then is called when the promise is rejected"() {
	given: "A promise with an doOnError consumer registered using then"
	MonoProcessor<String> promise = MonoProcessor.<String> create()
	def value
	promise.doOnSuccess {}.doOnError { value = it }.subscribeWith(MonoProcessor.create())

	when: "The promise is rejected"
	def e = new Exception()
	promise.onError e

	then: "the consumer is called"
	value == e
	thrown(Exception)
  }

  def "An onSuccess consumer registered via then is called when the promise is already fulfilled"() {
	given: "A promise that has been fulfilled"
	def promise = Mono.just('test')

	when: "An doOnSuccess consumer is registered via then"
	def value
	promise.doOnSuccess { value = it }.subscribeWith(MonoProcessor.create())

	then: "The consumer is called"
	value == 'test'
  }

  def "When a promise is fulfilled, if a mapping function throws an exception the mapped promise is rejected"() {
	given: "a promise with a filter that throws an error"
	MonoProcessor<String> promise = MonoProcessor.<String> create()
	def e = new RuntimeException()
	def mapped = MonoProcessor.create()
	promise.map { throw e }.subscribe(mapped)

	when: "the promise is fulfilled"
	promise.onNext 2
	mapped.request(1)

	then: "the mapped promise is rejected"
	thrown(Exception)
	mapped.error
  }

  def "When a promise is already fulfilled, if a mapping function throws an exception the mapped promise is rejected"() {
	given: "a fulfilled promise"
	def promise = Mono.just(1)

	when: "a mapping function that throws an error is added"
	def e = new RuntimeException()
	def mapped = MonoProcessor.create()
	promise.map { throw e }.subscribe(mapped)

	then: "the mapped promise is rejected"
	thrown(Exception)
	mapped.error
  }

  def "An IllegalStateException is thrown if an attempt is made to fulfil a fulfilled promise"() {
	given: "a fulfilled promise"
	def promise = MonoProcessor.<Integer> create()

	when: "an attempt is made to fulfil it"
	promise.onNext 1
	promise.onNext 1

	then: "an CancelException is thrown"
	thrown(Exceptions.CancelException)
  }

  def "An IllegalStateException is thrown if an attempt is made to reject a rejected promise"() {
	given: "a rejected promise"
	MonoProcessor promise = MonoProcessor.create()

	when: "an attempt is made to fulfil it"
	promise.onError new Exception()
	promise.onError new Exception()

	then: "an IllegalStateException is thrown"
	thrown(Exceptions.UpstreamException)
  }

  def "An IllegalStateException is thrown if an attempt is made to reject a fulfilled promise"() {
	given: "a fulfilled promise"
	def promise = MonoProcessor.create()

	when: "an attempt is made to fulfil it"
	promise.onNext 1
	promise.onError new Exception()

	then: "an IllegalStateException is thrown"
	thrown(Exceptions.UpstreamException)
  }

  def "Multiple promises can be combined"() {
	given: "two fulfilled promises"
	def bc1 = EmitterProcessor.<Integer> create().connect()
	def promise1 = bc1.doOnNext { println 'hey' + it }.next()
	def bc2 = MonoProcessor.<Integer> create()
	def promise2 = bc2.flux().log().next()

	when: "a combined promise is first created"
	def combined = Mono.when(promise1, promise2).subscribe()

	then: "it is pending"
	!combined.pending

	when: "the first promise is fulfilled"
	bc1.onNext 1

	then: "the combined promise is still pending"
	!combined.pending

	when: "the second promise if fulfilled"
	bc2.onNext 2

	then: "the combined promise is fulfilled with both values"
	combined.get().t1 == 1
	combined.get().t2 == 2
	combined.success
  }

  def "A combined promise is rejected once any of its component promises are rejected"() {
	given: "two unfulfilled promises"
	def promise1 = MonoProcessor.<Integer> create()
	def promise2 = MonoProcessor.<Integer> create()

	when: "a combined promise is first created"
	def combined = Mono.when(promise1, promise2).subscribeWith(MonoProcessor.create())

	then: "it is pending"
	!combined.pending

	when: "a component promise is rejected"
	promise1.onError new Exception()

	then: "the combined promise is rejected"
	thrown(Exception)
	combined.error
  }

  def "A combined promise is immediately fulfilled if its component promises are already fulfilled"() {
	given: "two fulfilled promises"
	def promise1 = Mono.just(1)
	def promise2 = Mono.just(2)

	when: "a combined promise is first created"
	def combined = Mono.when(promise1, promise2).subscribe()
	combined.peek()

	then: "it is fulfilled"
	combined.success
	combined.peek().t1 == 1
	combined.peek().t2 == 2

	when: "promises are supplied"
	promise1 = Mono.fromCallable { '1' }
	promise2 = Mono.fromCallable { '2' }
	combined = MonoProcessor.create()
	Mono.when(promise1, promise2).subscribe(combined)

	then: "it is fulfilled"
	combined.success
	combined.peek().t1 == '1'
	combined.peek().t2 == '2'

  }

  def "A combined promise through 'any' is fulfilled with the first component result when using synchronously"() {
	given: "two fulfilled promises"
	def promise1 = Mono.just(1)
	def promise2 = Mono.just(2)

	when: "a combined promise is first created"
	def combined = MonoProcessor.create()
	Mono.any(promise1, promise2).subscribe(combined)

	then: "it is fulfilled"
	combined.peek() == 1
	combined.success
  }

  def "A combined promise through 'any' is fulfilled with the first component result when using asynchronously"() {
	given: "two fulfilled promises"
	def ioGroup = SchedulerGroup.io("promise-task", 8, 2)
	def promise1 = Mono.fromCallable { sleep(10000); 1 }.subscribeOn(ioGroup)
	def promise2 = Mono.fromCallable { sleep(325); 2 }.subscribeOn(ioGroup)


	when: "a combined promise is first created"
	def combined =  Mono.any(promise1, promise2).subscribeWith(MonoProcessor.create())

	then: "it is fulfilled"
	combined.get(Duration.ofMillis(3205)) == 2
  }

  def "A combined promise is immediately rejected if its component promises are already rejected"() {
	given: "two rejected promises"
	def promise1 = Mono.error(new Exception())
	def promise2 = Mono.error(new Exception())

	when: "a combined promise is first created"
	Mono.when(promise1, promise2).get()
	println promise1.debug()
	println promise2.debug()

	then: "it is rejected"
	thrown Exception
  }

  def "A single promise can be 'combined'"() {
	given: "one unfulfilled promise"
	MonoProcessor<Integer> promise1 = MonoProcessor.create()

	when: "a combined promise is first created"
	def combined = MonoProcessor.create()
	Mono.when(promise1).log().subscribe(combined)

	then: "it is pending"
	!combined.pending

	when: "the first promise is fulfilled"
	println promise1.debug()
	promise1.onNext 1

	then: "the combined promise is fulfilled"
	combined.get(Duration.ofSeconds(1)) == [1]
	combined.success
  }

  def "A promise can be fulfilled with a Supplier"() {
	when: "A promise configured with a supplier"
	def promise = Mono.fromCallable { 1 }

	then: "it is fulfilled"
	promise.get() == 1
  }

  def "A promise with a Supplier that throws an exception is rejected"() {
	when: "A promise configured with a supplier that throws an error"
	def promise = Mono.fromCallable { throw new RuntimeException() }
	promise.get()

	then: "it is rejected"
	thrown RuntimeException
  }

  def "A filtered promise is not fulfilled if the filter does not allow the value to pass through"() {
	given: "a promise with a filter that only accepts even values"
	def promise = MonoProcessor.create()
	def filtered = promise.flux().filter { it % 2 == 0 }.next()

	when: "the promise is fulfilled with an odd value"
	promise.onNext 1

	then: "the filtered promise is not fulfilled"
	!filtered.get()
  }

  def "A filtered promise is fulfilled if the filter allows the value to pass through"() {
	given: "a promise with a filter that only accepts even values"
	def promise = MonoProcessor.create()
	promise.flux().filter { it % 2 == 0 }.next()

	when: "the promise is fulfilled with an even value"
	promise.onNext 2

	then: "the filtered promise is fulfilled"
	promise.success
	promise.peek() == 2
  }

  def "If a filter throws an exception the filtered promise is rejected"() {
	given: "a promise with a filter that throws an error"
	def promise = MonoProcessor.create()
	def e = new RuntimeException()
	def filteredMonoProcessor = promise.flux().filter { throw e }.next()

	when: "the promise is fulfilled"
	promise.onNext 2
	filteredMonoProcessor.get()

	then: "the filtered promise is rejected"
	thrown RuntimeException
  }

  def "If a promise is already fulfilled with a value accepted by a filter the filtered promise is fulfilled"() {
	given: "a promise that is already fulfilled with an even value"
	def promise = Mono.just(2)

	when: "the promise is filtered with a filter that only accepts even values"
	def v = promise.flux().filter { it % 2 == 0 }.next().get()

	then: "the filtered promise is fulfilled"
	2 == v
  }

  def "If a promise is already fulfilled with a value rejected by a filter, the filtered promise is not fulfilled"() {
	given: "a promise that is already fulfilled with an odd value"
	def promise = Mono.just(1)

	when: "the promise is filtered with a filter that only accepts even values"
	def v = promise.flux().filter { it % 2 == 0 }.next().get()

	then: "the filtered promise is not fulfilled"
	!v
  }

  def "Errors stop compositions"() {
	given: "a promise"
	def p1 = MonoProcessor.<String> create()

	final latch = new CountDownLatch(1)

	when: "p1 is consumed by p2"
	MonoProcessor p2 = p1.log().doOnSuccess({ Integer.parseInt it }).
			doOnError{ latch.countDown() }.
			log().
			map { println('not in log'); true }.subscribe()

	and: "setting a value"
	p1.onNext 'not a number'
	p2.get(1_000)

	then: 'No value'
	thrown(RuntimeException)
	latch.count == 0
  }

  def "Can poll instead of await to automatically handle InterruptedException"() {
	given: "a promise"
	def p1 = MonoProcessor.<String> create()

	when: "p1 is consumed by p2"
	def p2 = p1
			.publishOn(SchedulerGroup.single('test'))
			.map {
	  println Thread.currentThread();
	  sleep(3000);
	  Integer.parseInt it
	}

	and: "setting a value"
	p1.onNext '1'
	println "emitted"
	 p2.get(Duration.ofSeconds(1))

	then: 'No value'
	thrown IllegalStateException

	when: 'polling undefinitely'
	def v = p2.get()

	then: 'Value!'
	v
  }

  def "MonoProcessor should handle delayed subscriptions"() {
	given: "a promise delaying subscriptions"
	def m = new MonoDelayedSubscription()

	when: "we subscribe to that promise"
	def v = m.get(1_000)

	then: 'it should be subscribed to only once'
	m.subscriptionCount.get() == 1
	v == 0
  }

  class MonoDelayedSubscription extends Mono<Integer>{

	AtomicInteger subscriptionCount = new AtomicInteger()

	@Override
	void subscribe(Subscriber<? super Integer> s) {
	  Thread.start {
		DeferredScalarSubscriber<Integer, Integer> sds = new DeferredScalarSubscriber<>(s);
		// second subscription should be handled first
		// delaying the first subscription by 100ms, second by 50ms
		sleep((Long)(100 / (subscriptionCount.get() + 1)))
		s.onSubscribe(sds);
		def v = subscriptionCount.getAndIncrement()
		sds.complete(v)
	  }
	}
  }

}

