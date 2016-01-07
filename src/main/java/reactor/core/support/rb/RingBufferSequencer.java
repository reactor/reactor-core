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
package reactor.core.support.rb;

import org.reactivestreams.Subscriber;
import reactor.core.publisher.FluxFactory;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.support.rb.disruptor.RingBuffer;
import reactor.core.support.rb.disruptor.Sequence;
import reactor.core.support.rb.disruptor.Sequencer;
import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * @author Stephane Maldini
 */
public final class RingBufferSequencer<T>
		implements Consumer<SubscriberWithContext<T, Sequence>>,
		           Function<Subscriber<? super T>, Sequence> {

	private final RingBuffer<MutableSignal<T>> ringBuffer;
	private final long                         startSequence;

	public RingBufferSequencer(RingBuffer<MutableSignal<T>> ringBuffer, long startSequence) {
		this.ringBuffer = ringBuffer;
		this.startSequence = startSequence;
	}

	@Override
	public void accept(SubscriberWithContext<T, Sequence> subscriber) {
		final long cursor = subscriber.context().get() + 1L;

		if (cursor > ringBuffer.getCursor()) {
			subscriber.onComplete();
		}
		else {
			MutableSignal<T> signal = ringBuffer.get(cursor);
			RingBufferSubscriberUtils.route(signal, subscriber);
		}
		subscriber.context().set(cursor);
	}

	@Override
	public Sequence apply(Subscriber<? super T> subscriber) {
		if (ringBuffer == null) {
			throw FluxFactory.PrematureCompleteException.INSTANCE;
		}
		return Sequencer.newSequence(startSequence);
	}

	@Override
	public String toString() {
		return "ringbuffer=" + ringBuffer.toString();
	}
}