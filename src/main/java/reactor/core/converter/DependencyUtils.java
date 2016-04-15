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

package reactor.core.converter;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Utility class related to the various composition libraries supported.
 *
 * @author Stephane Maldini
 * @author Joao Pedro Evangelista
 * @since 2.5
 */
public final class DependencyUtils {

    static private final boolean HAS_REACTOR_IPC;
    static private final boolean HAS_REACTOR_NETTY;
    static private final boolean HAS_REACTOR_BUS;

    static private final FlowPublisherConverter      FLOW_PUBLISHER_CONVERTER;
    static private final RxJava1ObservableConverter  RX_JAVA_1_OBSERVABLE_CONVERTER;
    static private final RxJava1SingleConverter      RX_JAVA_1_SINGLE_CONVERTER;
    static private final RxJava1CompletableConverter RX_JAVA_1_COMPLETABLE_CONVERTER;

    private DependencyUtils() {
    }

    static {
        final int RXJAVA_1_OBSERVABLE = 0b000001;
        final int RXJAVA_1_SINGLE = 0b000010;
        final int RXJAVA_1_COMPLETABLE = 0b000100;
        final int FLOW_PUBLISHER = 0b100000;
        final int REACTOR_IPC = 0b1000000;
        final int REACTOR_BUS = 0b10000000;
        final int REACTOR_NETTY = 0b100000000;

        int detected = 0;
        try {
            Flux.class.getClassLoader()
                      .loadClass("rx.Observable");
            detected = RXJAVA_1_OBSERVABLE;
            Flux.class.getClassLoader()
                      .loadClass("rx.Single");
            detected |= RXJAVA_1_SINGLE;
            Flux.class.getClassLoader()
                      .loadClass("rx.Completable");
            detected |= RXJAVA_1_COMPLETABLE;
        }
        catch (ClassNotFoundException ignore) {

        }

        try {
            Flux.class.getClassLoader()
                      .loadClass("reactor.io.ipc.Channel");
            detected |= REACTOR_IPC;
        }
        catch (ClassNotFoundException ignore) {

        }

        try {
            Flux.class.getClassLoader()
                      .loadClass("reactor.io.netty.tcp.TcpServer");
            detected |= REACTOR_NETTY;
        }
        catch (ClassNotFoundException ignore) {

        }
        try {
            Flux.class.getClassLoader()
                      .loadClass("reactor.bus.registry.Registry");
            detected |= REACTOR_BUS;
        }
        catch (ClassNotFoundException ignore) {

        }

        if ((detected & RXJAVA_1_OBSERVABLE) == RXJAVA_1_OBSERVABLE) {
            RX_JAVA_1_OBSERVABLE_CONVERTER = RxJava1ObservableConverter.INSTANCE;
        }
        else {
            RX_JAVA_1_OBSERVABLE_CONVERTER = null;
        }
        if ((detected & RXJAVA_1_SINGLE) == RXJAVA_1_SINGLE) {
            RX_JAVA_1_SINGLE_CONVERTER = RxJava1SingleConverter.INSTANCE;
        }
        else {
            RX_JAVA_1_SINGLE_CONVERTER = null;
        }
        if ((detected & RXJAVA_1_COMPLETABLE) == RXJAVA_1_COMPLETABLE) {
            RX_JAVA_1_COMPLETABLE_CONVERTER = RxJava1CompletableConverter.INSTANCE;
        }
        else {
            RX_JAVA_1_COMPLETABLE_CONVERTER = null;
        }
        if ((detected & FLOW_PUBLISHER) == FLOW_PUBLISHER) {
            FLOW_PUBLISHER_CONVERTER = FlowPublisherConverter.INSTANCE;
        }
        else {
            FLOW_PUBLISHER_CONVERTER = null;
        }
        HAS_REACTOR_IPC = (detected & REACTOR_IPC) == REACTOR_IPC;
        HAS_REACTOR_BUS = (detected & REACTOR_BUS) == REACTOR_BUS;
        HAS_REACTOR_NETTY = (detected & REACTOR_NETTY) == REACTOR_NETTY;

    }

    public static boolean hasRxJava1() {
        return RX_JAVA_1_OBSERVABLE_CONVERTER != null;
    }

    public static boolean hasRxJava1Single() {
        return RX_JAVA_1_SINGLE_CONVERTER != null;
    }

    public static boolean hasFlowPublisher() {
        return FLOW_PUBLISHER_CONVERTER != null;
    }

    public static boolean hasRxJava1Completable() {
        return RX_JAVA_1_COMPLETABLE_CONVERTER != null;
    }

    public static boolean hasReactorIpc() {
        return HAS_REACTOR_IPC;
    }

    public static boolean hasReactorBus() {
        return HAS_REACTOR_BUS;
    }

    public static boolean hasReactorNetty() {
        return HAS_REACTOR_NETTY;
    }

    public static Publisher<?> convertToPublisher(Object source) {
        if (source == null) {
            throw new IllegalArgumentException("Cannot convert null sources");
        }

        if (hasRxJava1()) {
            if (hasRxJava1Single() && RX_JAVA_1_SINGLE_CONVERTER.test(source)) {
                return RX_JAVA_1_SINGLE_CONVERTER.apply(source);
            }
            else if (hasRxJava1Completable() && RX_JAVA_1_COMPLETABLE_CONVERTER.test(source)) {
                return RX_JAVA_1_COMPLETABLE_CONVERTER.apply(source);
            }
            else if (RX_JAVA_1_OBSERVABLE_CONVERTER.test(source)) {
                return RX_JAVA_1_OBSERVABLE_CONVERTER.apply(source);
            }
        }

        if (hasFlowPublisher() && FLOW_PUBLISHER_CONVERTER.test(source)) {
            return FLOW_PUBLISHER_CONVERTER.apply(source);
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
                return (T) RX_JAVA_1_SINGLE_CONVERTER.convertTo(source, to);
            }
            else if (hasRxJava1Completable() && RX_JAVA_1_COMPLETABLE_CONVERTER.get()
                                                                               .isAssignableFrom(to)) {
                return (T) RX_JAVA_1_COMPLETABLE_CONVERTER.convertTo(source, to);
            }
            else if (RX_JAVA_1_OBSERVABLE_CONVERTER.get()
                                                   .isAssignableFrom(to)) {
                return (T) RX_JAVA_1_OBSERVABLE_CONVERTER.convertTo(source, to);
            }
        }
        if (hasFlowPublisher() && FLOW_PUBLISHER_CONVERTER.get()
                                                          .isAssignableFrom(to)) {
            return (T) FLOW_PUBLISHER_CONVERTER.convertTo(source, to);
        }
        throw new UnsupportedOperationException("Cannot convert " + source.getClass() + " source to " + to.getClass() + " type");
    }
}
