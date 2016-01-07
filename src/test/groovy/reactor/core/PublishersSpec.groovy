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

import reactor.core.publisher.FluxLift
import reactor.core.subscriber.test.DataTestSubscriber
import spock.lang.Specification

import static reactor.Flux.fromIterable

/**
 * @author Stephane Maldini
 */
class PublishersSpec extends Specification {

  def "Error handling with onErrorReturn"() {

	given: "Iterable publisher of 1000 to read queue"
	def pub = fromIterable(1..1000).lift(FluxLift.lifter { d, s ->
	  if (d == 3) {
		throw new Exception('test')
	  }
	  s.onNext(d)
	})

	when: "read the queue"
	def s = DataTestSubscriber.createWithTimeoutSecs(3)
	pub.onErrorReturn(100000).subscribe(s)
	s.sendUnboundedRequest()

	then: "queues values correct"
	s.assertNextSignals(1, 2, 100000)
			.assertCompleteReceived()
  }

  def "Error handling with onErrorResume"() {

	given: "Iterable publisher of 1000 to read queue"
	def pub = fromIterable(1..1000).lift(FluxLift.lifter { d, s ->
	  if (d == 3) {
		throw new Exception('test')
	  }
	  s.onNext(d)
	})

	when: "read the queue"
	def s = DataTestSubscriber.createWithTimeoutSecs(3)
	pub.switchOnError(fromIterable(9999..10002)).subscribe(s)
	s.sendUnboundedRequest()

	then: "queues values correct"
	s.assertNextSignals(1, 2, 9999, 10000, 10001, 10002)
			.assertCompleteReceived()
  }

}
