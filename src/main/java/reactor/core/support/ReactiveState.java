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

package reactor.core.support;

import java.util.Iterator;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A component that supports extra state peeking and access for reactive components: buffers, capacity, names,
 * connected upstream/downstreams...
 *
 * The state read accuracy (volatility) is implementation-dependant and implementors MAY return cached value for a
 * given state.
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public interface ReactiveState {

	/*

	Capacity State : Buffer size (capacity), buffered,...

	 */

	/**
	 * A capacity aware component
	 */
	interface Bounded extends ReactiveState {

		/**
		 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription} request needs.
		 * This is the maximum in-flight data allowed to transit to this elements.
		 * @return long capacity
		 */
		long getCapacity();
	}

	/**
	 * A storing component
	 */
	interface Buffering extends Bounded {

		/**
		 * Return current used space in buffer
		 * @return long capacity
		 */
		long pending();
	}

	/*

	Upstream State : Publisher(S), outstanding request, ...

	 */

	/**
	 * A component that is linked to a source {@link Publisher}. Useful to traverse from left to right a pipeline of
	 * reactive actions implementing this interface.
	 */
	interface Upstream extends ReactiveState {

		/**
		 * Return the direct source of data, Supports reference
		 */
		Object upstream();
	}

	/**
	 * A component that is linked to N {@link Publisher}. Useful to traverse from left to right a pipeline of reactive
	 * actions implementing this interface.
	 */
	interface LinkedUpstreams extends ReactiveState {

		/**
		 * Return the connected sources of data
		 */
		Iterator<?> upstreams();

		/**
		 * @return the number of upstreams
		 */
		long upstreamsCount();
	}

	/**
	 * A request aware component
	 */
	interface UpstreamDemand extends ReactiveState {

		/**
		 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription} request needs.
		 * This is the maximum in-flight data allowed to transit to this elements.
		 * @return long capacity
		 */
		long expectedFromUpstream();
	}

	/**
	 * A request aware component
	 */
	interface UpstreamPrefetch extends UpstreamDemand {

		/**
		 *
		 * @return
		 */
		long limit();
	}

	/*

	Downstream State : Subscriber(S), Request from downstream...

	 */

	/**
	 * A component that is linked to N target {@link Subscriber}. Useful to traverse from right to left a pipeline of
	 * reactive actions implementing this interface.
	 */
	interface Downstream extends ReactiveState {

		/**
		 * Return the direct data receiver
		 */
		Object downstream();
	}

	/**
	 * A component that is linked to N target {@link Subscriber}. Useful to traverse from right to left a pipeline of
	 * reactive actions implementing this interface.
	 */
	interface LinkedDownstreams extends ReactiveState {

		/**
		 * @return the connected data receivers
		 */
		Iterator<?> downstreams();

		/**
		 * @return
		 */
		long downstreamsCount();

	}

	/**
	 * A request aware component
	 */
	interface DownstreamDemand extends ReactiveState {

		/**
		 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription} request needs.
		 * This is the maximum in-flight data allowed to transit to this elements.
		 * @return long capacity
		 */
		long requestedFromDownstream();
	}

	/*

	Running State : Name, controls (start, stop, pause),...

	 */

	/**
	 * An nameable component
	 */
	interface Named extends ReactiveState {

		/**
		 * Return defined name
		 */
		String getName();
	}

	/**
	 * An identifiable component
	 */
	interface Identified extends ReactiveState {

		/**
		 * Return defined id
		 */
		String getId();
	}

	/**
	 * A lifecycle backed upstream
	 */
	interface ActiveUpstream extends ReactiveState {

		/**
		 * @return
		 */
		boolean isStarted();

		/**
		 *
		 * @return
		 */
		boolean isTerminated();
	}

	/**
	 * A lifecycle backed upstream
	 */
	interface ActiveDownstream extends ReactiveState {

		/**
		 *
		 * @return
		 */
		boolean isCancelled();
	}

	/**
	 * A criteria grouped component
	 */
	interface Grouped<K> extends ReactiveState {

		/**
		 * Return defined identifier
		 */
		K key();
	}

	/**
	 * A component that is meant to be introspectable on finest logging level
	 */
	interface Trace extends ReactiveState {

	}

	/**
	 * A component that is meant to be embedded or gating user components
	 */
	interface Inner extends ReactiveState {

	}

	/**
	 * A component that holds a failure state if any
	 */
	interface FailState extends ReactiveState {

		Throwable getError();
	}

	/**
	 * A component that emits traces with the following standard :
	 * <pre>message, arg1: signalType, arg2: signalPayload and arg3: this</pre>
	 */
	interface Logging extends ReactiveState {

	}

	/**
	 * A component that is timed
	 */
	interface Timed extends ReactiveState {

		/**
		 * Can represent a period in milliseconds
		 * @return
		 */
		long period();
	}

	/**
	 * A component that is delegating to a sub-flow (processor, or publisher/subscriber chain)
	 */
	interface FeedbackLoop extends ReactiveState {

		Object delegateInput();

		Object delegateOutput();
	}

	/**
	 * A component that is intended to build others
	 */
	interface Factory extends ReactiveState {

	}

	/**
	 *
	 */
	interface Pausable extends ReactiveState {

		/**
		 * Cancel this {@literal Pausable}. The implementing component should never react to any stimulus,
		 * closing resources if necessary.
		 *
		 * @return {@literal this}
		 */
		Pausable cancel();

		/**
		 * Pause this {@literal Pausable}. The implementing component should stop reacting, pausing resources if necessary.
		 *
		 * @return {@literal this}
		 */
		Pausable pause();

		/**
		 * Unpause this {@literal Pausable}. The implementing component should resume back from a previous pause,
		 * re-activating resources if necessary.
		 *
		 * @return {@literal this}
		 */
		Pausable resume();

	}
	/**
	 * A simple interface that marks an object as being recyclable.
	 */

	interface Recyclable {

		/**
		 * Free any internal resources and reset the state of the object to enable reuse.
		 */
		void recycle();

	}

	/*
			Core System Env
	 */

	/**
	 *
	 */
	boolean TRACE_CANCEL = Boolean.parseBoolean(System.getProperty("reactor.trace.cancel", "false"));

	/**
	 *
	 */
	boolean TRACE_NOCAPACITY = Boolean.parseBoolean(System.getProperty("reactor.trace.nocapacity", "false"));

	/**
	 * An allocation friendly default of available slots in a given container, e.g. slow publishers and or fast/few
	 * subscribers
	 */
	int XS_BUFFER_SIZE     = 32;

	/**
	 * A small default of available slots in a given container, compromise between intensive pipelines, small
	 * subscribers numbers and memory use.
	 */
	int SMALL_BUFFER_SIZE  = 256;

	/**
	 * A larger default of available slots in a given container, e.g. mutualized processors, intensive pipelines or
	 * larger subscribers number
	 */
	int MEDIUM_BUFFER_SIZE = 8192;

	/**
	 * The size, in bytes, of a small buffer. Can be configured using the {@code reactor.io.defaultBufferSize} system
	 * property. Default to 16384 bytes.
	 */
	int SMALL_IO_BUFFER_SIZE = Integer.parseInt(System.getProperty("reactor.io.defaultBufferSize", "" + 1024 * 16));

	/**
	 * The maximum allowed buffer size in bytes. Can be configured using the {@code reactor.io.maxBufferSize} system
	 * property. Defaults to 16384000 bytes.
	 */
	int MAX_IO_BUFFER_SIZE = Integer.parseInt(System.getProperty("reactor.io.maxBufferSize", "" + 1024 * 1000 * 16));

	/**
	 *
	 */
	long DEFAULT_TIMEOUT = Long.parseLong(System.getProperty("reactor.await.defaultTimeout", "30000"));

	/**
	 * Whether the RingBuffer*Processor can be graphed by wrapping the individual Sequence with the target downstream
	 */
	boolean TRACEABLE_RING_BUFFER_PROCESSOR = Boolean.parseBoolean(System.getProperty("reactor.ringbuffer.trace",
			"true"));
}
