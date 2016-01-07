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

import reactor.Flux
import reactor.core.publisher.convert.CompletableFutureConverter
import reactor.core.subscriber.test.DataTestSubscriber
import rx.Observable
import rx.Single
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

import static reactor.Flux.fromIterable

/**
 * @author Stephane Maldini
 */
class PublisherConversionSpec extends Specification {

  def "From and To RxJava 1 Observable"() {

	given: "Iterable publisher of 1000 to read queue"
	def obs = Observable.range(1, 1000)
	def pub = Flux.convert(obs)
	def queue = DataTestSubscriber.createWithTimeoutSecs(3)


	when: "read the queue"
	pub.subscribe(queue)
	queue.sendUnboundedRequest()

	then: "queues values correct"
	queue.assertNextSignals(*(1..1000))
			.assertCompleteReceived()


	when: "Iterable publisher of 1000 to observable"
	pub = fromIterable(1..1000)
	obs = pub.convert(Observable.class)
	def blocking = obs.toList()

	def v = blocking.toBlocking().single()

	then: "queues values correct"
	v[0] == 1
	v[1] == 2
	v[999] == 1000
  }

  def "From and To RxJava 1 Single"() {

	given: "Iterable publisher of 1000 to read queue"
	def obs = Single.just(1)
	def pub = Flux.convert(obs)
	def queue = DataTestSubscriber.createWithTimeoutSecs(3)

	when: "read the queue"
	pub.subscribe(queue)
	queue.sendUnboundedRequest()

	then: "queues values correct"
	queue.assertNextSignals(1)
			.assertCompleteReceived()


	when: "Iterable publisher of 1000 to observable"
	pub = Flux.just(1)
	def single = pub.convert(Single.class)
	def blocking = single.toObservable().toBlocking()

	def v = blocking.single()

	then: "queues values correct"
	v == 1
  }

  def "From and To CompletableFuture"() {

	given: "Iterable publisher of 1 to read queue"
	def obs = CompletableFuture.completedFuture([1])
	def pub = Flux.<List<Integer>> convert(obs)
	def queue = DataTestSubscriber.createWithTimeoutSecs(3)

	when: "read the queue"
	pub.subscribe(queue)
	queue.sendUnboundedRequest()

	then: "queues values correct"
	queue.assertNextSignals([1])
			.assertCompleteReceived()


	when: "Iterable publisher of 1000 to completable future"
	pub = fromIterable(1..1000)
	obs = pub.<CompletableFuture<List<Integer>>> convert(CompletableFuture.class)
	def vList = obs.get()

	then: "queues values correct"
	vList[0] == 1
	vList[1] == 2
	vList[999] == 1000

	when: "Iterable publisher of 1 to completable future"
	def newPub = Flux.just(1)
	obs = CompletableFutureConverter.fromSingle(newPub)
	def v = obs.get()

	then: "queues values correct"
	v == 1
  }

/*

  def "From and To Flow Publisher"() {

	given: "submission publisher of 1000 to read queue"
	def source = new java.util.concurrent.SubmissionPublisher()
	def pub = Publishers.convert(source)
	def queue = toReadQueue(pub)

	when: "read the queue"
	def res = []
	1000.times {

	  source.submit(it)
	  res[it] = queue.take()
	}

	source.close()

	then: "queues values correct"
	res[0] == 0
	res[1] == 1
	res[999] == 999


	when: "Iterable publisher of 1000 to Flow Publisher"
	pub = from(1..1000)
	def obs = Publishers.convert(pub, java.util.concurrent.Flow.Publisher.class)
	res = []
	obs.subscribe(new java.util.concurrent.Flow.Subscriber<Object>() {
	  java.util.concurrent.Flow.Subscription s
	  @Override
	  void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
		this.s = subscription
		subscription.request(1L)
	  }

	  @Override
	  void onNext(Object o) {
		res << o
		s.request(1L)
	  }

	  @Override
	  void onError(Throwable throwable) {
		throwable.printStackTrace()
	  }

	  @Override
	  void onComplete() {
		println 'complete'
	  }
	})

	then: "queues values correct"
	res[0] == 1
	res[1] == 2
	res[999] == 1000
  }
*/


}
