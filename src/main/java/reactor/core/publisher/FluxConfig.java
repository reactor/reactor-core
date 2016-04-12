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

import org.reactivestreams.Publisher;
import reactor.core.flow.Fuseable;
import reactor.core.scheduler.TimedScheduler;
import reactor.core.state.Backpressurable;
import reactor.core.state.Introspectable;

/**
 * @param <T> the value type
 *
 * @since 2.5
 */
final class FluxConfig<T> extends FluxSource<T, T> {

	final long   capacity;
	final TimedScheduler  timer;
	final String name;

	static <T> Flux<T> withCapacity(Publisher<? extends T> source, long capacity) {
		if (source instanceof Fuseable) {
			return new FuseableFluxConfig<>(source,
					capacity,
					source instanceof Flux ? ((Flux) source).getTimer() : null,
					source instanceof Introspectable ? ((Introspectable) source).getName() : source.getClass()
					                                                                               .getSimpleName());
		}
		return new FluxConfig<>(source,
				capacity,
				source instanceof Flux ? ((Flux) source).getTimer() : null,
				source instanceof Introspectable ? ((Introspectable) source).getName() : source.getClass()
				                                                                               .getSimpleName());
	}

	static <T> Flux<T> withTimer(Publisher<? extends T> source, TimedScheduler timer) {
		if (source instanceof Fuseable) {
			return new FuseableFluxConfig<>(source,
					source instanceof Backpressurable ? ((Backpressurable) source).getCapacity() : -1L,
					timer,
					source instanceof Introspectable ? ((Introspectable) source).getName() : source.getClass()
					                                                                               .getSimpleName());
		}
		return new FluxConfig<>(source,
				source instanceof Backpressurable ? ((Backpressurable) source).getCapacity() : -1L,
				timer,
				source instanceof Introspectable ? ((Introspectable) source).getName() : source.getClass()
				                                                                               .getSimpleName());
	}

	static <T> Flux<T> withName(Publisher<? extends T> source, String name) {
		if (source instanceof Fuseable) {
			return new FuseableFluxConfig<>(source,
					source instanceof Backpressurable ? ((Backpressurable) source).getCapacity() : -1L,
					source instanceof Flux ? ((Flux) source).getTimer() : null,
					name);
		}
		return new FluxConfig<>(source,
				source instanceof Backpressurable ? ((Backpressurable) source).getCapacity() : -1L,
				source instanceof Flux ? ((Flux) source).getTimer() : null,
				name);
	}

	public FluxConfig(Publisher<? extends T> source, long capacity, TimedScheduler timer, String name) {
		super(source);
		this.capacity = capacity;
		this.timer = timer;
		this.name = name;
	}

	@Override
	public TimedScheduler getTimer() {
		return timer;
	}

	@Override
	public long getCapacity() {
		return capacity;
	}

	@Override
	public String getName() {
		return name;
	}
}

final class FuseableFluxConfig<I> extends FluxSource<I, I> implements Fuseable {

	final long           capacity;
	final TimedScheduler timer;
	final String         name;

	public FuseableFluxConfig(Publisher<? extends I> source, long capacity, TimedScheduler timer, String name) {
		super(source);
		this.capacity = capacity;
		this.timer = timer;
		this.name = name;
	}

	@Override
	public TimedScheduler getTimer() {
		return timer;
	}

	@Override
	public long getCapacity() {
		return capacity;
	}

	@Override
	public String getName() {
		return name;
	}
}