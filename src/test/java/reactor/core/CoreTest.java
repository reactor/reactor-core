/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core;

import java.util.Collections;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephane Maldini
 */
public class CoreTest {

	@Test
	public void defaultDisposable(){
		Disposable d = () -> {};
		assertThat(d.isDisposed()).isFalse();
	}

	@Test
	public void defaultLoopback(){
		Loopback d = new Loopback() {};
		assertThat(d.connectedInput()).isNull();
		assertThat(d.connectedOutput()).isNull();
	}

	@Test
	public void defaultMultiProducer(){
		MultiProducer d = () -> null;
		assertThat(d.downstreams()).isNull();
		assertThat(d.hasDownstreams()).isFalse();
		assertThat(d.downstreamCount()).isEqualTo(-1);
	}

	@Test
	public void defaultMultiReceiver(){
		MultiReceiver d = () -> null;
		assertThat(d.upstreams()).isNull();
		assertThat(d.upstreamCount()).isEqualTo(-1);
	}

	@Test
	public void defaultTrackable(){
		Trackable d = new Trackable() {};
		assertThat(d.expectedFromUpstream()).isEqualTo(-1);
		assertThat(d.getCapacity()).isEqualTo(-1);
		assertThat(d.getError()).isNull();
		assertThat(d.getPending()).isEqualTo(-1);
		assertThat(d.isCancelled()).isFalse();
		assertThat(d.isStarted()).isFalse();
		assertThat(d.isTerminated()).isFalse();
		assertThat(d.limit()).isEqualTo(-1);
		assertThat(d.requestedFromDownstream()).isEqualTo(-1);
	}

	@Test
	public void defaultFuseableQueueSubscription(){
		TestQueueSubscription tqs = new TestQueueSubscription();
		testUnsupported(() -> tqs.peek());
		testUnsupported(() -> tqs.add(0));
		testUnsupported(() -> tqs.addAll(Collections.emptyList()));
		testUnsupported(() -> tqs.offer(0));
		testUnsupported(() -> tqs.retainAll(Collections.emptyList()));
		testUnsupported(() -> tqs.remove());
		testUnsupported(() -> tqs.remove(0));
		testUnsupported(() -> tqs.removeAll(Collections.emptyList()));
		testUnsupported(() -> tqs.element());
		testUnsupported(() -> tqs.contains(0));
		testUnsupported(() -> tqs.containsAll(Collections.emptyList()));
		testUnsupported(() -> tqs.iterator());
		testUnsupported(() -> tqs.toArray((Integer[])null));
		testUnsupported(() -> tqs.toArray());
		testUnsupported(() -> tqs.peek());
	}

	final void testUnsupported(Runnable r){
		try{
			r.run();
			fail();
		}
		catch (UnsupportedOperationException uoe){
			//IGNORE
		}
	}

	static final class TestQueueSubscription implements Fuseable.QueueSubscription<Integer> {

		@Override
		public int requestFusion(int requestedMode) {
			return 0;
		}

		@Override
		public Integer poll() {
			return null;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public void clear() {

		}

		@Override
		public void request(long n) {

		}

		@Override
		public void cancel() {

		}
	}
}
