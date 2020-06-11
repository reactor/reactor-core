/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

/**
 * A collection of standalone sinks ({@link reactor.core.publisher.SinkFlux.Standalone}).
 *
 * @author Simon Baslé
 */
public final class Sinks {

	private Sinks() { }

	/**
	 * Accumulates elements that are fed to the {@link SinkFlux.Standalone} before any subscriber is subscribed.
	 *
	 * @param <T>
	 * @return
	 */
	public static final <T> SinkFlux.Standalone<T> coldFlux() {
		return new FluxProcessorSink<>(Processors.replayUnbounded());
	}

	public static final <T> SinkFlux.Standalone<T> coldUnicastFlux() {
		return new FluxProcessorSink<>(Processors.unicast());
	}

	/**
	 * Feeding elements to the {@link SinkFlux.Standalone} while there is no registered subscriber results in dropping of these elements.
	 * Calling a terminal method while there is no registered subscriber will retain that termination and immediately replay it once
	 * a subscriber comes in.
	 *
	 * @param <T>
	 * @return
	 */
	public static final <T> SinkFlux.Standalone<T> hotFlux() {
		return new FluxProcessorSink<>(Processors.more().replaySize(0, false));
	}

	static final class FluxProcessorSink<T>
			implements SinkFlux.Standalone<T> {

		final FluxSink<T>         delegateSink;
		final FluxProcessor<T, T> processor;

		@SuppressWarnings("deprecation")
		FluxProcessorSink(FluxProcessor<T, T> processor) {
			this.processor = processor;
			this.delegateSink = processor.sink();
		}

		@Override
		public Flux<T> asFlux() {
			return processor;
		}

		@Override
		public void complete() {
			delegateSink.complete();
		}

		@Override
		public void error(Throwable e) {
			delegateSink.error(e);
		}

		@Override
		public FluxSink<T> next(T t) {
			return delegateSink.next(t);
		}
	}

}
