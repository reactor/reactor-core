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

import org.reactivestreams.Publisher
import org.reactivestreams.Subscription
import reactor.core.publisher.*
import reactor.core.scheduler.Schedulers
import reactor.test.subscriber.AssertSubscriber
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiFunction
import java.util.function.Function
import java.util.function.Supplier

import static reactor.core.publisher.Flux.error

class FluxSpec extends Specification {

	def 'Collect will accumulate multiple lists of accepted values and pass it to a consumer on bucket close'() {
		given:
			'a source and a collected flux'
			def numbers = EmitterProcessor.<Integer> create().connect()

		when:
			'non overlapping buffers'
			def boundaryFlux = EmitterProcessor.<Integer> create().connect()
			def res = numbers.buffer ( boundaryFlux ).buffer().publishNext()
			res.subscribe()

			numbers.onNext(1)
			numbers.onNext(2)
			numbers.onNext(3)
			boundaryFlux.onNext(1)
			numbers.onNext(5)
			numbers.onNext(6)
			numbers.onComplete()

		then:
			'the collected lists are available'
			res.block() == [[1, 2, 3], [5, 6]]

		when:
			'overlapping buffers'
			def bucketOpening = EmitterProcessor.<Integer> create().connect()
			boundaryFlux = EmitterProcessor.<Integer> create(false).connect()
			numbers = EmitterProcessor.<Integer> create().connect()
			res = numbers.log('numb').buffer(bucketOpening, { boundaryFlux.log('boundary') } as Function).log('promise').buffer()
					.publishNext()

			res.subscribe()

			numbers.onNext(1)
			numbers.onNext(2)
			bucketOpening.onNext(1)
			numbers.onNext(3)
			bucketOpening.onNext(1)
			numbers.onNext(5)
			boundaryFlux.onNext(1)
			bucketOpening.onNext(1)
			numbers.onNext(6)
			boundaryFlux.onComplete()
			numbers.onComplete()


		then:
			'the collected overlapping lists are available'
			res.block() == [[3, 5], [5], [6]]
	}

	def 'Window will reroute multiple stream of accepted values and pass it to a consumer'() {
		given:
			'a source and a collected flux'
			def numbers = Flux.fromIterable([1, 2, 3, 4, 5, 6, 7, 8])

		when:
			'non overlapping buffers'
			def res = numbers.log('n').window(2, 3).log('test.').flatMap { it.log('fm')
					.buffer() }.buffer().next()

		then:
			'the collected lists are available'
			res.block() == [[1, 2], [4, 5], [7, 8]]

		when:
			'overlapping buffers'
			res = numbers.log("n").window(3, 2).log('w').flatMap { it.log('b').collectList() }.collectList()

		then:
			'the collected overlapping lists are available'
			res.block() == [[1, 2, 3], [3, 4, 5], [5, 6, 7], [7, 8]]


		when:
			'non overlapping buffers'
			res = numbers.delay(Duration.ofMillis(90)).window(Duration.ofMillis(200),
					Duration.ofMillis(300))
					.flatMap{ it.log('fm').collectList() }
					.collectList().cache().block()

		then:
			'the collected lists are available'
			res == [[1, 2], [4, 5], [7, 8]] || res == [[1, 2, 3], [4, 5], [7, 8]]

	}


	def 'Re route will accumulate multiple lists of accepted values and pass it to a consumer on bucket close'() {
		given:
			'a source and a collected flux'
			def numbers = EmitterProcessor.<Integer> create().connect()

		when:
			'non overlapping buffers'
			def boundaryFlux = EmitterProcessor.<Integer> create(false).connect()
			def res = numbers.window(boundaryFlux).flatMap { it.buffer().log() }.buffer().publishNext()
			res.subscribe()

			numbers.onNext(1)
			numbers.onNext(2)
			numbers.onNext(3)
			boundaryFlux.onNext(1)
			numbers.onNext(5)
			numbers.onNext(6)
			numbers.onComplete()

		then:
			'the collected lists are available'
			res.block() == [[1, 2, 3], [5, 6]]

		when:
			'overlapping buffers'
			numbers = EmitterProcessor.<Integer> create().connect()
			def bucketOpening = EmitterProcessor.<Integer> create().connect()
			res = numbers.log("w").window(bucketOpening.log("bucket")) { boundaryFlux.log('boundary') }.flatMap { it
					.log('fm').collectList() }
					.collectList().cache()

			res.subscribe()

			numbers.onNext(1)
			numbers.onNext(2)
			bucketOpening.onNext(1)
			numbers.onNext(3)
			bucketOpening.onNext(1)
			numbers.onNext(5)
			boundaryFlux.onNext(1)
			bucketOpening.onNext(1)
			numbers.onNext(6)
			numbers.onComplete()


		then:
			'the collected overlapping lists are available'
			res.block() == [[3, 5], [5], [6]]
	}

	def 'Window will re-route N elements to a fresh nested stream'() {
		given:
			'a source and a collected window flux'
			def source = EmitterProcessor.<Integer> create().connect()
			def value = null

			source.log('w').window(2).subscribe { s ->
				s.log().buffer(2).subscribe{
				  value = it
				}
			}


		when:
			'the first value is accepted on the source'
			source.onNext(1)

		then:
			'the collected list is not yet available'
			!value

		when:
			'the second value is accepted'
			source.onNext(2)

		then:
			'the collected list contains the first and second elements'
			value == [1, 2]

		when:
			'2 more values are accepted'
			source.onNext(3)
			source.onNext(4)

		then:
			'the collected list contains the first and second elements'
			value == [3, 4]
	}

	def 'Window will re-route N elements over time to a fresh nested stream'() {
		given:
			'a source and a collected window flux'
			def source = EmitterProcessor.<Integer> create().connect()
			def promise = MonoProcessor.create()

			source.publishOn(Schedulers.parallel()).log("prewindow").window(Duration.ofSeconds(10)).subscribe {
				it.log().buffer(2).subscribe { promise.onNext(it) }
			}


		when:
			'the first value is accepted on the source'
			source.onNext(1)

		then:
			'the collected list is not yet available'
			promise.peek() == null

		when:
			'the second value is accepted'
			source.onNext(2)

		then:
			'the collected list contains the first and second elements'
			promise.block() == [1, 2]

		when:
			'2 more values are accepted'
			promise = MonoProcessor.create()

			sleep(2000)
			source.onNext(3)
			source.onNext(4)

		then:
			'the collected list contains the first and second elements'
			promise.block() == [3, 4]
	}

	def 'GroupBy will re-route N elements to a nested stream based on the mapped key'() {
		given:
			'a source and a grouped by ID flux'
			def source = EmitterProcessor.<SimplePojo> create().connect()
			def result = [:]

			source.groupBy { pojo ->
				pojo.id
			}.subscribe { stream ->
				stream.subscribe { pojo ->
					if (result[pojo.id]) {
						result[pojo.id] << pojo.title
					} else {
						result[pojo.id] = [pojo.title]
					}
				}
			}


		when:
			'some values are accepted'
			source.onNext(new SimplePojo(id: 1, title: 'Stephane'))
			source.onNext(new SimplePojo(id: 1, title: 'Jon'))
			source.onNext(new SimplePojo(id: 1, title: 'Sandrine'))
			source.onNext(new SimplePojo(id: 2, title: 'Acme'))
			source.onNext(new SimplePojo(id: 3, title: 'Acme2'))
			source.onNext(new SimplePojo(id: 3, title: 'Acme3'))
			source.onComplete()

		then:
			'the result should group titles by id'
			result
			result == [
					1: ['Stephane', 'Jon', 'Sandrine'],
					2: ['Acme'],
					3: ['Acme2', 'Acme3']
			]
	}

	def 'GroupBy will re-route N elements to a nested stream based on hashcode'() {
		given:
			'a source and a grouped by ID flux'
			def source = EmitterProcessor.<SimplePojo> create().connect()
			def result = [:]
			def latch = new CountDownLatch(6)

			def partitionFlux = source.publishOn(Schedulers.parallel()).parallel().groups()
			partitionFlux.subscribe { stream ->
				stream.cast(SimplePojo).subscribe { pojo ->
					if (result[pojo.id]) {
						result[pojo.id] << pojo.title
					} else {
						result[pojo.id] = [pojo.title]
					}
					latch.countDown()
				}
			}

		when:
			'some values are accepted'
			source.onNext(new SimplePojo(id: 1, title: 'Stephane'))
			source.onNext(new SimplePojo(id: 1, title: 'Jon'))
			source.onNext(new SimplePojo(id: 1, title: 'Sandrine'))
			source.onNext(new SimplePojo(id: 2, title: 'Acme'))
			source.onNext(new SimplePojo(id: 3, title: 'Acme2'))
			source.onNext(new SimplePojo(id: 3, title: 'Acme3'))


		then:
			'the result should group titles by id'
			latch.await(5, TimeUnit.SECONDS)
			result
			result == [
					1: ['Stephane', 'Jon', 'Sandrine'],
					2: ['Acme'],
					3: ['Acme2', 'Acme3']
			]
	}

	def 'Creating Stream from publisher'() {
		given:
			'a source flux with a given publisher'
			def s = Flux.<String> create {
				it.next('test1')
				it.next('test2')
				it.next('test3')
				it.complete()
			}.log()

		when:
			'accept a value'
			def result = s.collectList().block(Duration.ofSeconds(5))

		then:
			'dispatching works'
			result == ['test1', 'test2', 'test3']
	}

	def 'Creating Stream from publisher factory'() {
		given:
			'a source flux with a given publisher'
			def scheduler = Schedulers.newParallel("work", 4)
			def s = Flux.create {  sub  ->
						(1..3).each {
							sub.next("test$it")
						}
						sub.complete()
					}
					.log()
					.subscribeOn(scheduler)

		when:
			'accept a value'
			def result = s.collectList().block(Duration.ofSeconds(5))

		then:
			'dispatching works'
			result == ['test1', 'test2', 'test3']
	  	cleanup:
			scheduler.dispose()
	}


	def 'Defer Stream from publisher'() {
		given:
			'a source flux with a given publisher factory'
			def i = 0
			def res = []
			def s = Flux.<Integer> defer {
				Flux.just(i++)
			}.log()

		when:
			'accept a value'
			res << s.next().block(Duration.ofSeconds(5))
			res << s.next().block(Duration.ofSeconds(5))
			res << s.next().block(Duration.ofSeconds(5))

		then:
			'dispatching works'
			res == [0, 1, 2]
	}

	def 'Wrap Stream from publisher'() {
		given:
			'a source flux with a given publisher factory'
			def terminated = false
			def s = Flux.<Integer> from {
				it.onSubscribe(new Subscription() {
					@Override
					void request(long n) {
						it.onNext(1)
						it.onNext(2)
						it.onNext(3)
						it.onComplete()
					}

					@Override
					void cancel() {
					}
				})
			}.log()

		when:
			'accept a value'
			def res = s.collectList().block(Duration.ofSeconds(5))

		then:
			'dispatching works'
			res == [1, 2, 3]
	}

	def 'Creating Stream from Timer'() {
		given:
			'a source flux with a given globalTimer'

			def res = 0l
			def c = Mono.delayMillis(1000)
			def timeStart = System.currentTimeMillis()

		when:
			'consuming'
			c.block()
			res = System.currentTimeMillis() - timeStart

		then:
			'create'
			res > 800

		when:
			'consuming periodic'
			def i = []
			c = Flux.intervalMillis(0, 1000).log().subscribe {
				i << it
			}
			sleep(2500)
			c.dispose()

		then:
			'create'
			i.containsAll([0l, 1l])
	}


	def 'Creating Stream from range'() {
		given:
			'a source flux with a given range'
			def s = Flux.range(1, 10)

		when:
			'accept a value'
			def result = s.collectList().block()

		then:
			'dispatching works'
			result == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
	}

	def 'Counting Stream from range'() {
		given:
			'a source flux with a given range'
			def s = Flux.range(1, 10)

		when:
			'accept a value'
			def result = s.count().block()

		then:
			'dispatching works'
			result == 10
	}

	def 'Creating Empty Streams from publisher'() {
		given:
			'a source flux pre-completed'
			def s = Flux.empty()

		when:
			'consume it'
			def latch = new CountDownLatch(1)
			def nexts = []
			def errors = []

			s.subscribe(
					{ nexts << 'never' },
					{ errors << 'never ever' },
					{ latch.countDown() }
			)

		then:
			'dispatching works'
			latch.await(2, TimeUnit.SECONDS)
			!nexts
			!errors
	}

	def 'Caching Stream from publisher'() {
		given:
			'a slow source flux'
			def s = Flux.<Integer> create {
				it.next 1
				sleep 200
				it.next 2
				sleep 200
				it.next 3
				it.complete()
			}.log('beforeCache')
					.cache()
					.log('afterCache')
					.elapsed()
					.map { it.t1 }

		when:
			'consume it twice'
			def latch = new CountDownLatch(2)
			def nexts = []
			def errors = []

			s.subscribe(
					{ nexts << it },
					{ errors << it },
					{ latch.countDown() }
			)

			s.subscribe(
					{ nexts << it },
					{ errors << it },
					{ latch.countDown() }
			)

		then:
			'dispatching works'
			latch.await(2, TimeUnit.SECONDS)
			!errors
			nexts.size() == 6
			nexts[0] + nexts[1] + nexts[2] >= 400
			nexts[3] + nexts[4] + nexts[5] < 50
	}

	def 'Caching Stream from publisher with ttl'() {
		given:
			'a slow source flux'
			def s = Flux.<Integer> create {
				it.next 1
				sleep 200
				it.next 2
				sleep 200
				it.next 3
				it.complete()
			}.log('beforeCache')
					.cache(Duration.ofMillis(300))
					.log('afterCache')
					.elapsed()

		when:
			'consume it twice'
			def latch = new CountDownLatch(2)
			def nexts = []
			def errors = []

			s.subscribe(
					{ nexts << it },
					{ errors << it },
					{ latch.countDown() }
			)

			s.subscribe(
					{ nexts << it },
					{ errors << it },
					{ latch.countDown() }
			)

		then:
			'dispatching works'
			latch.await(2, TimeUnit.SECONDS)
			!errors
			nexts.size() == 5
			nexts[0].t1 + nexts[1].t1 + nexts[2].t1 >= 400
			nexts[3].t1 + nexts[4].t1  < 50
	}

	def 'Creating Mono from CompletableFuture'() {
		given:
			'a source flux pre-completed'
			def executorService = Executors.newSingleThreadExecutor()

			def future = CompletableFuture.supplyAsync({
				'hello future'
			} as Supplier<String>, executorService)

			def s = Mono.fromFuture(future).as{ Flux.from(it) }

		when:
			'consume it'
			def latch = new CountDownLatch(1)
			def nexts = []
			def errors = []

			s.subscribe(
					{ nexts << it },
					{ errors << it; it.printStackTrace() },
					{ latch.countDown() }
			)
			def counted = latch.await(3, TimeUnit.SECONDS)
		then:
			'dispatching works'
			!errors
			nexts[0] == 'hello future'
			counted

		cleanup:
			executorService.shutdown()
	}

	def 'Throttle will accumulate a list of accepted values and pass it to a consumer on the specified period'() {
		given:
			'a source and a collected flux'
			def source = EmitterProcessor.<Integer> create().connect()
			def reduced = source.buffer(2).log().delay(Duration.ofMillis(300))
			def value
			reduced.subscribe{ value = it }

		when:
			'the first values are accepted on the source'
			source.onNext(1)
			source.onNext(2)
			sleep(1200)

		then:
			'the collected list is available'
			value == [1, 2]

		when:
			'the second value is accepted'
			source.onNext(3)
			source.onNext(4)
			sleep(1200)

		then:
			'the collected list contains the first and second elements'
			value == [3, 4]
	}

	def 'Throttle will generate demand every specified period'() {
		given:
			'a source and a collected flux'
			def random = new Random()
			def source = Flux.create {
				it.next random.nextInt()
			}.publishOn(Schedulers.parallel())

			def values = []

			source.delay(Duration.ofMillis(200)).subscribe {
				values << it
			}

		when:
			'the first values are accepted on the source'
			sleep(1200)

		then:
			'the collected list is available'
			values

	}

	def 'Collect with Timeout will accumulate a list of accepted values and pass it to a consumer'() {
		given:
			'a source and a collected flux'
			def source = EmitterProcessor.<Integer> create().connect()
			def reduced = source.buffer(5, Duration.ofMillis(600))
			def ts = reduced.subscribeWith(AssertSubscriber.create())

		when:
			'the first values are accepted on the source'
			source.onNext(1)
			source.onNext(1)
			source.onNext(1)
			source.onNext(1)
			source.onNext(1)
			sleep(2000)

		and:
			'the second value is accepted'
			source.onNext(2)
			source.onNext(2)
			sleep(2000)

		then:
			'the collected list contains the first and second elements'
			ts.assertValues([1, 1, 1, 1, 1], [2, 2])

	}

	def 'Timeout can be bound to a stream'() {
		given:
			'a source and a timeout'
			def source = EmitterProcessor.<Integer> create()
			def reduced = source.timeout(Duration.ofMillis(1500))
			def error = null
			def value
			reduced.onErrorResumeWith(){
				error = it
			  Flux.just(-5)
			}.subscribe{ value = it }

		when:
			'the first values are accepted on the source, paused just enough to refresh globalTimer until 6'
			source.onNext(1)
			sleep(500)
			source.onNext(2)
			source.onNext(3)
			source.onNext(4)
			sleep(500)
			source.onNext(5)
			sleep(2000)
			source.onNext(6)
			println error

		then:
			'last value known is 5 and flux is in error state'
			error in TimeoutException
			value == -5
	}

	def 'Timeout can be bound to a stream and fallback'() {
		given:
			'a source and a timeout'
			def source = EmitterProcessor.<Integer> create().connect()
			def reduced = source.timeout(Duration.ofMillis(1500), Flux.just(10))
			def error = null
			def value
			reduced.doOnError(TimeoutException) {
				error = it
			}.subscribe{ value = it  }

		when:
			'the first values are accepted on the source, paused just enough to refresh globalTimer until 6'
			source.onNext(1)
			sleep(500)
			source.onNext(2)
			source.onNext(3)
			source.onNext(4)
			source.onNext(5)
			sleep(2000)
			source.onNext(6)

		then:
			'last value known is 10 as the flux has used its fallback'
			!error
			value == 10
	}

	def 'Errors can have a fallback'() {
		when:
			'A source flux emits next signals followed by an error'
			def res = []
			def myFlux = Flux.create { aSubscriber ->
				aSubscriber.next('Three')
				aSubscriber.next('Two')
				aSubscriber.next('One')
				aSubscriber.error(new Exception())
			}

		and:
			'A fallback flux will emit values and complete'
			def myFallback = Flux.create { aSubscriber ->
				aSubscriber.next('0')
				aSubscriber.next('1')
				aSubscriber.next('2')
				aSubscriber.complete()
			}

		and:
			'fallback flux is assigned to source flux on any error'
			myFlux.switchOnError(myFallback).subscribe(
					{ println(it); res << it },                          // onNext
					{ println("Error: " + it.message) }, // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == ['Three', 'Two', 'One', '0', '1', '2', 'complete']
	}


	def 'Errors can have a fallback return value'() {
		when:
			'A source flux emits next signals followed by an error'
			def res = []
			def myFlux = Flux.create { aSubscriber ->
				aSubscriber.next('Three')
				aSubscriber.next('Two')
				aSubscriber.next('One')
				aSubscriber.error(new Exception())
			}

		and:
			'A fallback value will emit values and complete'
			myFlux.onErrorReturn('Zero').subscribe(
					{ println(it); res << it },                          // onNext
					{ println("Error: " + it.message) }, // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == ['Three', 'Two', 'One', 'Zero', 'complete']
	}

	def 'Streams can be materialized'() {
		when:
			'A source flux emits next signals followed by complete'
			List res = []
			def myFlux = Flux.create { aSubscriber ->
				aSubscriber.next('Three')
				aSubscriber.next('Two')
				aSubscriber.next('One')
				aSubscriber.complete()
			}

		and:
			'A materialized flux is consumed'
			myFlux.materialize().subscribe(
					{ println(it); res << it.toString() },                          // onNext
					{ it.printStackTrace() },                          // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == ['onNext(Three)',
					'onNext(Two)',
					'onNext(One)',
					'onComplete()',
					'complete']

		when:
			'A source flux emits next signals followed by complete'
			res = []
			myFlux = Flux.create { aSubscriber ->
				aSubscriber.next('Three')
				aSubscriber.error(new Exception())
			}

		and:
			'A materialized flux is consumed'
			myFlux.materialize().subscribe(
					{ println(it); res << it.toString() },                          // onNext
					{ it.printStackTrace() },                          // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == ['onNext(Three)',
					'onError(java.lang.Exception)',
					'complete']
	}

	def 'Streams can be dematerialized'() {
		when:
			'A source flux emits next signals followed by complete'
			def res = []
			def myFlux = Flux.create { aSubscriber ->
				aSubscriber.next(Signal.next(1))
				aSubscriber.next(Signal.next(2))
				aSubscriber.next(Signal.next(3))
				aSubscriber.next(Signal.complete())
			}

		and:
			'A dematerialized flux is consumed'
			myFlux.dematerialize().subscribe(
					{ println(it); res << it },                          // onNext
					{ it.printStackTrace() },                          // onError
					{ println("Sequence complete"); res << 'complete' }          // onComplete
			)

		then:
			res == [1, 2, 3, 'complete']
	}


	def 'Streams can be switched'() {
		when:
			'A source flux emits next signals followed by an error'
			def res = []
			def myFlux = Flux.create { aSubscriber ->
				aSubscriber.next('Three')
				aSubscriber.next('Two')
				aSubscriber.next('One')
			}

		and:
			'Another flux will emit values and complete'
			def myFallback = Flux.create { aSubscriber ->
				aSubscriber.next('0')
				aSubscriber.next('1')
				aSubscriber.next('2')
				aSubscriber.complete()
			}

		and:
			'The streams are switched'
			def switched = Flux.switchOnNext(Flux.just(myFlux, myFallback))
			switched.subscribe(
					{ println(Thread.currentThread().name + ' ' + it); res << it },                          // onNext
					{ println("Error: " + it.message) }, // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == ['Three', 'Two', 'One', '0', '1', '2', 'complete']
	}

	def 'Streams can be switched dynamically'() {
		when:
			'A source flux emits next signals followed by an error'
			def res = []
			def myFlux = Flux.<Integer> create { aSubscriber ->
				aSubscriber.next(1)
				aSubscriber.next(2)
				aSubscriber.next(3)
				aSubscriber.complete()
			}

		and:
			'The streams are switched'
			def switched = myFlux.log('lol').switchMap { Flux.range(it, 3) }.log("after-lol")
			switched.subscribe(
					{ println(Thread.currentThread().name + ' ' + it); res << it },                          // onNext
					{ println("Error: " + it.message) }, // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == [1, 2, 3, 2, 3, 4, 3, 4, 5, 'complete']
	}


	def 'onOverflowDrop will miss events non requested'() {
		given:
			'a source and a timeout'
			def source = EmitterProcessor.<Integer> create().connect()

			def value = null
			def s = source.log("drop").onBackpressureDrop().doOnNext { value = it }.log('overflow-drop-test')
			def tail = s.subscribeWith(AssertSubscriber.create(0))


			tail.request(5)

		when:
			'the first values are accepted on the source, but we only have 5 requested elements out of 6'
			source.onNext(1)
			source.onNext(2)
			source.onNext(3)
			source.onNext(4)
			source.onNext(5)

			source.onNext(6)

		and:
			'we try to consume the tail to check if 6 has been buffered'
			tail.request(1)

		then:
			'last value known is 5'
			value == 5
	}

  def 'onBackpressureBuffer will block events non requested'() {
	given:
	'a source and a timeout'
	def source = EmitterProcessor.<Integer> create().connect()

	def value = null
	def tail = AssertSubscriber.create(5)

	source
			.log("block")
			.publishOn(Schedulers.parallel())
			.onBackpressureBuffer()
			.doOnNext { value = it }
			.log('overflow-block-test')
			.delay(Duration.ofMillis(100))
			.subscribe(tail)


	when:
	'the first values are accepted on the source, but we only have 5 requested elements out of 6'
	source.onNext(1)
	source.onNext(2)
	source.onNext(3)
	source.onNext(4)
	source.onNext(5)


	then:
	tail.awaitAndAssertNextValues(1,2,3,4,5)

	when:
	'we try to consume the tail to check if 6 has been buffered'
	source.onNext(6)
	tail.request(1)
	source.onComplete()

	then:
	'last value known is 5'
	tail.await()
		.assertValueCount(6)
		.assertComplete()

	when:
	'we try to consume the tail to check if 6 has been buffered'
	source.onNext(6)
	tail.request(1)
	source.onComplete()

	then:
	'last value known is 5'
	tail.await()
		.assertValueCount(6)
		.assertComplete()
  }

  def 'onBackpressureBuffer will error with bounded and events non requested'() {
	given:
	'a source and a timeout'
	def source = DirectProcessor.<Integer> create()

	def tail = AssertSubscriber.create(0)

	def end = 0
	source
			.log("block")
			.onBackpressureBuffer(8)
			.log("after")
			.subscribe(tail)


	when:
	'the first values are accepted on the source, but we only have 5 requested elements out of 6'
	9.times { source.onNext(it) }

	then:
	'last value known is 5'
	tail.assertError().assertValueCount(0)

  }
  def 'onBackpressureBuffer will error with bounded, callback, events non requested'() {
	given:
	'a source and a timeout'
	def source = DirectProcessor.<Integer> create()

	def tail = AssertSubscriber.create(0)

	def end = 0
	source
			.log("block")
			.onBackpressureBuffer(8, { end = it })
			.log("after")
			.subscribe(tail)


	when:
	'the first values are accepted on the source, but we only have 5 requested elements out of 6'
	9.times { source.onNext(it) }

	then:
	'last value known is 5'
	end == 8

	when: ' request'
	tail.request(Long.MAX_VALUE)

	then:
	'last value known is 5'
	tail.assertError().assertValues(0, 1, 2, 3, 4, 5, 6, 7)

  }

	def 'A Flux can be throttled'() {
		given:
			'a source and a throttled flux'
			def source = EmitterProcessor.<Integer> create().connect()
			long avgTime = 150l

			def reduced = source
					.delay(Duration.ofMillis(avgTime))
					.elapsed()
					.log('el')
					.take(10)
					.reduce(0l) { acc, next ->
				acc > 0l ? ((next.t1 + acc) / 2) : next.t1
			}

			def value = reduced.log().subscribeWith(MonoProcessor.create())

		when:
			'the first values are accepted on the source'
			for (int i = 0; i < 10000; i++) {
				source.onNext(1)
			}
			sleep(1500)
			println(((long) (value.block())) + " milliseconds on average")

		then:
			'the average elapsed time between 2 signals is greater than throttled time'
			value.peek() >= avgTime * 0.6

	}

	def 'time-slices of average'() {
		given:
			'a source and a throttled flux'
			def source = EmitterProcessor.<Integer> create().connect()
			def latch = new CountDownLatch(1)
			long avgTime = 150l

			def reduced = source
					.buffer()
					.delay(Duration.ofMillis(avgTime))
					.map { timeWindow -> timeWindow.size() }
					.doAfterTerminate { latch.countDown() }

			def value
			reduced.subscribe{ value = it }

		when:
			'the first values are accepted on the source'
			for (int i = 0; i < 10000; i++) {
				source.onNext(1)
			}
			source.onComplete()
			latch.await(10, TimeUnit.SECONDS)

		then:
			'the average elapsed time between 2 signals is greater than throttled time'
			value > 1

	}


	def 'Collect will accumulate values from multiple threads'() {
		given:
			'a source and a collected flux'
			def sum = new AtomicInteger()
			int length = 1000
			int batchSize = 333
			def i = new AtomicInteger()
			int latchCount = length / batchSize
			def latch = new CountDownLatch(latchCount)
			def head = EmitterProcessor.<Integer> create().connect()
			head
					.publishOn(Schedulers.parallel())
			.doOnNext{ c -> i.incrementAndGet() }
					.take(1000)
					.parallel(3)
					.runOn(Schedulers.parallel())
					.map { it }
					.collect({[]}, {c, v -> c << v})
					.subscribe({ List<Integer> ints ->
								println ints.size()
								sum.addAndGet(ints.size())
								latch.countDown()
					}, { e -> println e }, { _it -> println "passe"} )
		when:
			'values are accepted into the head'
		try {
		  (1..length).each { head.onNext(it) }
		}
		catch (Exception e){
		  println i
		  Exceptions.unwrap(e).printStackTrace()
		  throw e
		}
			latch.await()

		then:
			'results contains the expected values'
			sum.get() == length
	}

	def 'A Flux can be timestamped'() {
		given:
			'a composable with an initial value and a relative time'
			def stream = Flux.just('test')
			def timestamp = System.currentTimeMillis()

		when:
			'timestamp operation is added and the flux is retrieved'
			def value = stream.timestamp().next().block()

		then:
			'it is available'
			value.t1 >= timestamp
			value.t2 == 'test'
	}

	def 'A Flux can be benchmarked'() {
		given:
			'a composable with an initial value and a relative time'
			def stream = Flux.just('test')
			long timestamp = System.currentTimeMillis()

		when:
			'elapsed operation is added and the flux is retrieved'
			def value = stream.doOnNext {
				sleep(1000)
			}.elapsed().next().block()

			long totalElapsed = System.currentTimeMillis() - timestamp

		then:
			'it is available'
			value.t1 <= totalElapsed
			value.t2 == 'test'
	}

	def 'A Flux can be sorted'() {
		given:
			'a composable with an initial value and a relative time'
			Flux<Integer> stream = Flux.fromIterable([43, 32122, 422, 321, 43, 443311])

		when:
			'sorted operation is added and the flux is retrieved'
			def value = stream.log().collectSortedList().cache().subscribe().block()

		then:
			'it is available'
			value == [43, 43, 321, 422, 32122, 443311]

		when:
			'a composable with an initial value and a relative time'
			stream = Flux.fromIterable([1, 2, 3, 4])

		and:
			'revese sorted operation is added and the flux is retrieved'
			value = stream
					.collectSortedList({ a, b -> b <=> a } as Comparator<Integer>)
					.block()

		then:
			'it is available'
			value == [4, 3, 2, 1]
	}

	def 'A Flux can be limited'() {
		given:
			'a composable with an initial values'
			def stream = Flux.fromIterable(['test', 'test2', 'test3'])

		when:
			'take to the first 2 elements'
			def value
			stream.take(2).subscribe{ value = it }

		then:
			'the second is the last available'
			value == 'test2'

		when:
			'take until test2 is seen'
			def stream2 = EmitterProcessor.create().connect()
			def value2
			stream2.log().takeWhile {
				'test2' != it
			}.subscribe{ value2 = it }


		stream2.onNext('test1')
		stream2.onNext('test2')
		stream2.onNext('test3')

		then:
			'the second is the last available'
			value2 == 'test1'
	}


	def 'A Flux can be limited in time'() {
		given:
			'a composable with an initial values'
			def stream = Flux.range(0, Integer.MAX_VALUE)
					.publishOn(Schedulers.parallel())

		when:
			'take to the first 2 elements'
		stream.take(Duration.ofSeconds(2)).then().block()

		then:
			'the second is the last available'
			notThrown(Exception)
	}


	def 'A Flux can be skipped'() {
		given:
			'a composable with an initial values'
			def stream = Flux.fromIterable(['test', 'test2', 'test3'])

		when:
			'skip to the second element'
			def value = stream.skip(1).collectList().block()

		then:
			'the first has been skipped'
			value == ['test2', 'test3']

		when:
			'skip until test2 is seen'
			stream = ReplayProcessor.create().connect()
			def value2 = stream.skipWhile {
				'test1' == it
			}.log("test").collectList()

			stream.subscribe()

			stream.onNext('test1')
			stream.onNext('test2')
			stream.onNext('test3')
			stream.onComplete()

		then:
			'the second is the last available'
			value2.block() == ['test2', 'test3']
	}


	def 'A Flux can be skipped in time'() {
		given:
			'a composable with an initial values'
			def stream = Flux.range(0, 1000)
					.publishOn(Schedulers.parallel())

		when:
			def promise = stream.log("skipTime").skip(Duration.ofSeconds(2)).collectList()

		then:
			!promise.block()
	}/*

	def "A Codec output can be streamed"() {
		given: "A delimiter stripping decoder and a buffer of delimited data"
			def codec = new DelimitedCodec<String, String>(true, StandardCodecs.STRING_CODEC)
			def string = 'Hello World!\nHello World!\nHello World!\n'
			def data1 = Buffer.wrap(string)
			string = 'Test\nTest\n'
			def data2 = Buffer.wrap(string)
			string = 'Test\nEnd\n'
			def data3 = Buffer.wrap(string)

		when: "data flux is decoded"
			def res = Streams.just(data1, data2, data3)
					.decode(codec)
					.collectList()
					.await(5, TimeUnit.SECONDS)

		then: "the buffers have been correctly decoded"
			res == ['Hello World!', 'Hello World!', 'Hello World!', 'Test', 'Test', 'Test', 'End']
	}
*/

  def "error publishers don't fast fail"(){
	when: 'preparing error publisher'
		Publisher<Object> publisher = error(new IllegalStateException("boo"))
	Flux.from(publisher).onErrorReturn{ ex -> "error"}
	    def a = 1

	then: 'no exceptions'
		a == 1
  }

	static class SimplePojo {
		int id
		String title

		int hashcode() { id }
	}

	static class Reduction implements BiFunction<Integer, Integer, Integer> {
		@Override
		 Integer apply(Integer left, Integer right) {
			def result = right == null ? 1 : left * right
			println "${right} ${left} reduced to ${result}"
			return result
		}
	}
}
