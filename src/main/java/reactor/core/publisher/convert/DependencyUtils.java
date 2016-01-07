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

package reactor.core.publisher.convert;

import org.reactivestreams.Publisher;
import reactor.Flux;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public final class DependencyUtils {

	static private final String VERSION = "2.5.0.BUILD-SNAPSHOT";

	static private final boolean HAS_REACTOR_STREAM;
	static private final boolean HAS_REACTOR_CODEC;
	static private final boolean HAS_REACTOR_NET;
	static private final boolean HAS_REACTOR_BUS;

	static private final CompletableFutureConverter COMPLETABLE_FUTURE_CONVERTER;
	static private final Jdk9FlowConverter          JDK_9_FLOW_CONVERTER;
	static private final RxJava1Converter           RX_JAVA_1_CONVERTER;
	static private final RxJava1SingleConverter     RX_JAVA_1_SINGLE_CONVERTER;

	private DependencyUtils() {
	}

	static {
		final int RXJAVA_1_OBSERVABLE = 0b000001;
		final int RXJAVA_1_SINGLE = 0b000010;
		final int RXJAVA_1_COMPLETABLE = 0b000100;
		final int REACTOR_STREAM = 0b001000;
		final int JDK8_COMPLETABLE_FUTURE = 0b010000;
		final int JDK9_FLOW = 0b100000;
		final int REACTOR_CODEC = 0b1000000;
		final int REACTOR_BUS = 0b10000000;
		final int REACTOR_NET = 0b100000000;

		int detected = 0;
		try {
			Class.forName("rx.Observable");
			detected = RXJAVA_1_OBSERVABLE;
			Class.forName("rx.Single");
			detected |= RXJAVA_1_SINGLE;
			/*Class.forName("rx.Completable");
			hasRxjava1Completable = true;*/
		}
		catch (ClassNotFoundException cnfe) {
			//IGNORE
		}
		try {
			Class.forName("java.util.concurrent.CompletableFuture");
			detected |= JDK8_COMPLETABLE_FUTURE;
			Class.forName("java.util.concurrent.Flow");
			detected |= JDK9_FLOW;
		}
		catch (SecurityException | ClassNotFoundException cnfe) {
			//IGNORE
		}
		try {
			Class.forName("reactor.rx.Stream");
			detected |= REACTOR_STREAM;
		}
		catch (ClassNotFoundException cnfe) {
			//IGNORE
		}
		try {
			Class.forName("reactor.io.codec.Codec");
			detected |= REACTOR_CODEC;
		}
		catch (ClassNotFoundException cnfe) {
			//IGNORE
		}
		try {
			Class.forName("reactor.io.net.ReactiveChannel");
			detected |= REACTOR_NET;
		}
		catch (ClassNotFoundException cnfe) {
			//IGNORE
		}
		try {
			Class.forName("reactor.bus.registry.Registry");
			detected |= REACTOR_BUS;
		}
		catch (ClassNotFoundException cnfe) {
			//IGNORE
		}

		if ((detected & RXJAVA_1_OBSERVABLE) == RXJAVA_1_OBSERVABLE) {
			RX_JAVA_1_CONVERTER = RxJava1Converter.INSTANCE;
		}
		else {
			RX_JAVA_1_CONVERTER = null;
		}
		if ((detected & RXJAVA_1_SINGLE) == RXJAVA_1_SINGLE) {
			RX_JAVA_1_SINGLE_CONVERTER = RxJava1SingleConverter.INSTANCE;
		}
		else {
			RX_JAVA_1_SINGLE_CONVERTER = null;
		}
		if ((detected & RXJAVA_1_COMPLETABLE) == RXJAVA_1_COMPLETABLE) {
			//TBD
		}
		else {
			//TBD
		}
		if ((detected & JDK8_COMPLETABLE_FUTURE) == JDK8_COMPLETABLE_FUTURE) {
			COMPLETABLE_FUTURE_CONVERTER = CompletableFutureConverter.INSTANCE;
		}
		else {
			COMPLETABLE_FUTURE_CONVERTER = null;
		}
		if ((detected & JDK9_FLOW) == JDK9_FLOW) {
			JDK_9_FLOW_CONVERTER = Jdk9FlowConverter.INSTANCE;
		}
		else {
			JDK_9_FLOW_CONVERTER = null;
		}
		HAS_REACTOR_STREAM = (detected & REACTOR_STREAM) == REACTOR_STREAM;
		HAS_REACTOR_CODEC = (detected & REACTOR_CODEC) == REACTOR_CODEC;
		HAS_REACTOR_BUS = (detected & REACTOR_BUS) == REACTOR_BUS;
		HAS_REACTOR_NET = (detected & REACTOR_NET) == REACTOR_NET;

	}

	public static boolean hasRxJava1() {
		return RX_JAVA_1_CONVERTER != null;
	}

	public static boolean hasRxJava1Single() {
		return RX_JAVA_1_SINGLE_CONVERTER != null;
	}

	public static boolean hasJdk8CompletableFuture() {
		return COMPLETABLE_FUTURE_CONVERTER != null;
	}

	public static boolean hasJdk9Flow() {
		return JDK_9_FLOW_CONVERTER != null;
	}

	public static boolean hasReactorStream() {
		return HAS_REACTOR_STREAM;
	}

	public static boolean hasReactorCodec() {
		return HAS_REACTOR_CODEC;
	}

	public static boolean hasReactorBus() {
		return HAS_REACTOR_BUS;
	}

	public static boolean hasReactorNet() {
		return HAS_REACTOR_NET;
	}

	public static Publisher<?> convertToPublisher(Object source) {
		if (source == null) {
			throw new IllegalArgumentException("Cannot convert null sources");
		}
		if (hasRxJava1()) {
			if (hasRxJava1Single() && RX_JAVA_1_SINGLE_CONVERTER.test(source)) {
				return RX_JAVA_1_SINGLE_CONVERTER.apply(source);
			}
			else if (RX_JAVA_1_CONVERTER.test(source)) {
				return RX_JAVA_1_CONVERTER.apply(source);
			}
		}

		if (hasJdk8CompletableFuture() && COMPLETABLE_FUTURE_CONVERTER.test(source)) {
			return COMPLETABLE_FUTURE_CONVERTER.apply(source);
		}

		if (hasJdk9Flow() && JDK_9_FLOW_CONVERTER.test(source)) {
			return JDK_9_FLOW_CONVERTER.apply(source);
		}
		throw new UnsupportedOperationException("Conversion to Publisher from " + source.getClass());
	}

	@SuppressWarnings("unchecked")
	public static <T> T convertFromPublisher(Publisher<?> source, Class<T> to) {
		if (source == null || to == null) {
			throw new IllegalArgumentException("Cannot convert " + source + " source to " + to + " type");
		}
		if (hasRxJava1()) {
			if (hasRxJava1Single() && RX_JAVA_1_SINGLE_CONVERTER.get()
			                                                    .isAssignableFrom(to)) {
				return (T) RX_JAVA_1_SINGLE_CONVERTER.apply(source, to);
			}
			else if (RX_JAVA_1_CONVERTER.get()
			                            .isAssignableFrom(to)) {
				return (T) RX_JAVA_1_CONVERTER.apply(source, to);
			}
		}

		if (hasJdk8CompletableFuture() && COMPLETABLE_FUTURE_CONVERTER.get()
		                                                              .isAssignableFrom(to)) {
			return (T) COMPLETABLE_FUTURE_CONVERTER.apply(source, to);
		}

		if (hasJdk9Flow() && JDK_9_FLOW_CONVERTER.get()
		                                         .isAssignableFrom(to)) {
			return (T) JDK_9_FLOW_CONVERTER.apply(source, to);
		}
		throw new UnsupportedOperationException("Cannot convert " + source.getClass() + " source to " + to.getClass() + " type");
	}

	public static String reactorVersion() {
		return VERSION;
	}
}
