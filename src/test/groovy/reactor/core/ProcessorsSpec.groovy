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
import reactor.Processors
import reactor.core.processor.ProcessorGroup
import reactor.core.processor.RingBufferProcessor
import reactor.core.processor.RingBufferWorkProcessor
import reactor.fn.BiConsumer
import reactor.fn.Consumer
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

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
				println name + " " + Thread.currentThread().name + ": subscribe: " + s
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
			def bc = RingBufferProcessor.<String> create(executorService, 16)
			//def bc2 = RingBufferProcessor.<String> create(executorService, 16)
			def elems = 100
			def latch = new CountDownLatch(elems)

			//bc.subscribe(bc2)
			//bc2.subscribe(sub('spec1', latch))
			bc.subscribe(sub('spec1', latch))
			bc.start()

		when:
			"call the processor"
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
			bc = RingBufferProcessor.<String> create(executorService)
			bc.subscribe(sub('spec2', latch))
			bc.start()

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
			def bc = RingBufferWorkProcessor.<String> create(executorService, 16)
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
			"call the processor"
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
			def sameThread = ProcessorGroup.sync().dataDispatcher()
			BiConsumer<String, Consumer<String>> diffThread = Processors.ioGroup("rbWork").dataDispatcher()
			def currentThread = Thread.currentThread()
			Thread taskThread = null
			Consumer<String> consumer = { ev ->
				taskThread = Thread.currentThread()
			}

		when:
			"a task is submitted"
			sameThread.accept('test', consumer)

		then:
			"the task thread should be the current thread"
			currentThread == taskThread

		when:
			"a task is submitted to the thread pool dispatcher"
			def latch = new CountDownLatch(1)
			diffThread.accept('test', { String ev -> consumer.accept(ev); latch.countDown() } as Consumer<String>)

			latch.await(5, TimeUnit.SECONDS) // Wait for task to execute

		then:
			"the task thread should be different when the current thread"
			taskThread != currentThread
			//!diffThread.shutdown()


		cleanup:
			ProcessorGroup.release(diffThread)
	}

	def "Dispatcher thread can be reused"() {

		given:
			"ring buffer eventBus"
			def serviceRB = Processors.singleGroup("rb", 32)
			def r = serviceRB.dataDispatcher()
			def latch = new CountDownLatch(2)

		when:
			"listen for recursive event"
			Consumer<Integer> c
			c = { data ->
				if (data < 2) {
					latch.countDown()
					r.accept(++data, c,)
				}
			}

		and:
			"call the eventBus"
			r.accept(0, c)

		then:
			"a task is submitted to the thread pool dispatcher"
			latch.await(5, TimeUnit.SECONDS) // Wait for task to execute

		cleanup:
			ProcessorGroup.release(r)
	}

	def "Dispatchers can be shutdown awaiting tasks to complete"() {

		given:
			"a Reactor with a ThreadPoolExecutorDispatcher"
			def serviceRB = Processors.ioGroup("rbWork", 32)
			def r = serviceRB.dataDispatcher()
			long start = System.currentTimeMillis()
			def hello = ""
			def latch = new CountDownLatch(1)
			def c = { String ev ->
				hello = ev
				Thread.sleep(1000)
			  	latch.countDown()
			} as Consumer<String>

		when:
			"the Dispatcher is shutdown and tasks are awaited"
			r.accept("Hello World!", c)
			def success = serviceRB.awaitAndShutdown(5, TimeUnit.SECONDS)
			long end = System.currentTimeMillis()
		then:
			"the Consumer was run, this thread was blocked, and the Dispatcher is shut down"
			hello == "Hello World!"
			success
			(end - start) >= 1000

	}

	def "RingBufferDispatcher executes tasks in correct thread"() {

		given:
			def serviceRB = Processors.singleGroup("rb", 8)
			def dispatcher = serviceRB.executor()
			def t1 = Thread.currentThread()
			def t2 = Thread.currentThread()

		when:
			dispatcher.execute({ t2 = Thread.currentThread() })
			Thread.sleep(500)

		then:
			t1 != t2

		cleanup:
			ProcessorGroup.release(dispatcher)

	}

	def "WorkQueueDispatcher executes tasks in correct thread"() {

		given:
			def serviceRBWork = Processors.ioGroup("rbWork", 1024, 8)
			def dispatcher = serviceRBWork.executor()
			def t1 = Thread.currentThread()
			def t2 = Thread.currentThread()

		when:
			dispatcher.execute({ t2 = Thread.currentThread() })
			Thread.sleep(500)

		then:
			t1 != t2

		cleanup:
			ProcessorGroup.release(dispatcher)

	}

	def "MultiThreadDispatchers support ping pong dispatching"(BiConsumer<String, Consumer<? super String>> d) {
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
					d.accept("pong", pong)
					latch.countDown()
				}
			}
			pong = {
				if (latch.count > 0) {
					t2 = Thread.currentThread()
					d.accept("ping", ping)
					latch.countDown()
				}
			}

			d.accept("ping", ping)

		then:
			latch.await(1, TimeUnit.SECONDS)
			main != t1
			main != t2

		cleanup:
		 ProcessorGroup.release(d)

		where:
			d << [
					ProcessorGroup.create(RingBufferWorkProcessor.create("rbWork", 1024), 4).dataDispatcher(),
			]

	}
}
