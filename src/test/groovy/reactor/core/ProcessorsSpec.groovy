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
import org.reactivestreams.Subscription
import reactor.core.publisher.TopicProcessor
import reactor.core.publisher.WorkQueueProcessor
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/**
 * @author Stephane Maldini
 */
class ProcessorsSpec extends Specification {

	@Shared
	ExecutorService executorService

	def setup() {
		executorService = Executors.newFixedThreadPool(2)
	}

	def cleanup() {
		executorService.shutdown()
	}

	private sub(String name, CountDownLatch latch) {
		new Subscriber<String>() {
			def s

			@Override
			void onSubscribe(Subscription s) {
				this.s = s
				println name + " " + Thread.currentThread().name + ": subscribe: " + s.toString()
				s.request(1)
			}

			@Override
			void onNext(String o) {
				println name + " " + Thread.currentThread().name + ": next: " + o
				latch.countDown()
				s.request(1)
			}

			@Override
			void onError(Throwable t) {
				println name + " " + Thread.currentThread().name + ": error:" + t
				t.printStackTrace()
			}

			@Override
			void onComplete() {
				println name + " " + Thread.currentThread().name + ": complete"
				//latch.countDown()
			}
		}
	}

	def "Dispatcher on Reactive Stream"() {

		given:
			"ring buffer processor with 16 backlog size"
			def bc = TopicProcessor.<String> create(executorService, 16)
			//def bc2 = TopicProcessor.<String> create(executorService, 16)
			def elems = 100
			def latch = new CountDownLatch(elems)

			//bc.subscribe(bc2)
			//bc2.subscribe(sub('spec1', latch))
			bc.subscribe(sub('spec1', latch))
			bc.connect()

		when:
			"createWorker the processor"
			elems.times {
				bc.onNext 'hello ' + it
			}
			bc.onComplete()

			def ended = latch.await(50, TimeUnit.SECONDS) // Wait for task to execute

		then:
			"a task is submitted to the thread pool dispatcher"
			ended

		when:
			latch = new CountDownLatch(elems)
			bc = TopicProcessor.<String> create("test", 16)
			bc.subscribe(sub('spec2', latch))
			bc.connect()

			elems.times {
				bc.onNext 'hello ' + it
			}
			bc.onComplete()

		then:
			"a task is submitted to the thread pool dispatcher"
			latch.await(50, TimeUnit.SECONDS) // Wait for task to execute
	}

	def "Work Processor on Reactive Stream"() {

		given:
			"ring buffer processor with 16 backlog size"
			def bc = WorkQueueProcessor.<String> create(executorService, 16)
			def elems = 18
			def latch = new CountDownLatch(elems)
			def manualSub = new Subscription() {
				@Override
				void request(long n) {
					println Thread.currentThread().name + " $n"
				}

				@Override
				void cancel() {
					println Thread.currentThread().name + " cancelling"
				}
			}

			//bc.subscribe(bc2)
			//bc2.subscribe(sub('spec1', latch))
			bc.subscribe(sub('spec1', latch))
			bc.onSubscribe(manualSub)

		when:
			"createWorker the processor"
			elems.times {
				bc.onNext 'hello ' + it
			}
			bc.onComplete()

			def ended = latch.await(50, TimeUnit.SECONDS) // Wait for task to execute

		then:
			"a task is submitted to the thread pool dispatcher"
			ended
	}

	def "Dispatcher executes tasks in correct thread"() {

		given:
		def diffThread = Schedulers.newParallel('work', 2).createWorker()
			def currentThread = Thread.currentThread()
			Thread taskThread = null


		when:
			"a task is submitted to the thread pool dispatcher"
			def latch = new CountDownLatch(1)
		diffThread.schedule { taskThread = Thread.currentThread() }
		diffThread.schedule { taskThread = Thread.currentThread(); latch.countDown() }

			latch.await(5, TimeUnit.SECONDS) // Wait for task to execute

		then:
			"the task thread should be different when the current thread"
			taskThread != currentThread
			//!diffThread.dispose()


		cleanup:
			diffThread.dispose()
	}

	def "Dispatcher thread can be reused"() {

		given:
			"ring buffer eventBus"
			def serviceRB = Schedulers.newParallel("rb", 4)
		def r = serviceRB.createWorker()
			def latch = new CountDownLatch(2)

		when:
			"listen for recursive event"
			Consumer<Integer> c
			c = { data ->
				if (data < 2) {
					latch.countDown()
				  r.schedule { c.accept(++data) }
				}
			}

		and:
			"createWorker the eventBus"
		r.schedule { c.accept(0) }

		then:
			"a task is submitted to the thread pool dispatcher"
			latch.await(5, TimeUnit.SECONDS) // Wait for task to execute

		cleanup:
			r.dispose()
	}

	def "RingBufferDispatcher executes tasks in correct thread"() {

		given:
			def serviceRB = Schedulers.newSingle("rb")
		def dispatcher = serviceRB.createWorker()
			def t1 = Thread.currentThread()
			def t2 = Thread.currentThread()

		when:
		dispatcher.schedule({ t2 = Thread.currentThread() })
			Thread.sleep(500)

		then:
			t1 != t2

		cleanup:
			dispatcher.dispose()

	}

	def "WorkQueueDispatcher executes tasks in correct thread"() {

		given:
			def serviceRBWork = Schedulers.newParallel("rbWork", 8)
		def dispatcher = serviceRBWork.createWorker()
			def t1 = Thread.currentThread()
			def t2 = Thread.currentThread()

		when:
		dispatcher.schedule({ t2 = Thread.currentThread() })
			Thread.sleep(500)

		then:
			t1 != t2

		cleanup:
		dispatcher.dispose()

	}

  def "MultiThreadDispatchers support ping pong dispatching"(Scheduler.Worker d) {
		given:
			def latch = new CountDownLatch(4)
			def main = Thread.currentThread()
			def t1 = Thread.currentThread()
			def t2 = Thread.currentThread()

		when:
			Consumer<String> pong

			Consumer<String> ping = {
				if (latch.count > 0) {
					t1 = Thread.currentThread()
				  d.schedule { pong.accept("pong") }
					latch.countDown()
				}
			}
			pong = {
				if (latch.count > 0) {
					t2 = Thread.currentThread()
				  d.schedule { ping.accept("ping") }
					latch.countDown()
				}
			}

		d.schedule { ping.accept("ping") }

		then:
			latch.await(1, TimeUnit.SECONDS)
			main != t1
			main != t2

		cleanup:
		 d.dispose()

		where:
			d << [Schedulers.newParallel("rbWork", 8).createWorker()]

	}
}
