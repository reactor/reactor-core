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

import java.util.concurrent.Flow;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import rx.Completable;
import rx.Observable;
import rx.Single;

/**
 * Utility class related to the various composition libraries supported.
 *
 * @author Stephane Maldini
 * @author Joao Pedro Evangelista
 * @since 2.5
 */
@SuppressWarnings("unchecked")
public enum Converters {
    ;

    static private final boolean HAS_REACTOR_IPC;
    static private final boolean HAS_REACTOR_NETTY;
    static private final boolean HAS_REACTOR_BUS;

    static private final FlowPublisherConverterWrapper      FLOW_PUBLISHER_CONVERTER;
    static private final RxJava1ObservableConverterWrapper  RXJAVA1_OBSERVABLE_CONVERTER;
    static private final RxJava1SingleConverterWrapper      RXJAVA1_SINGLE_CONVERTER;
    static private final RxJava1CompletableConverterWrapper RXJAVA1_COMPLETABLE_CONVERTER;




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
            RXJAVA1_OBSERVABLE_CONVERTER = new RxJava1ObservableConverterWrapper();
        }
        else {
            RXJAVA1_OBSERVABLE_CONVERTER = null;
        }
        if ((detected & RXJAVA_1_SINGLE) == RXJAVA_1_SINGLE) {
            RXJAVA1_SINGLE_CONVERTER = new RxJava1SingleConverterWrapper();
        }
        else {
            RXJAVA1_SINGLE_CONVERTER = null;
        }
        if ((detected & RXJAVA_1_COMPLETABLE) == RXJAVA_1_COMPLETABLE) {
            RXJAVA1_COMPLETABLE_CONVERTER = new RxJava1CompletableConverterWrapper();
        }
        else {
            RXJAVA1_COMPLETABLE_CONVERTER = null;
        }
        if ((detected & FLOW_PUBLISHER) == FLOW_PUBLISHER) {
            FLOW_PUBLISHER_CONVERTER = new FlowPublisherConverterWrapper();
        }
        else {
            FLOW_PUBLISHER_CONVERTER = null;
        }
        HAS_REACTOR_IPC = (detected & REACTOR_IPC) == REACTOR_IPC;
        HAS_REACTOR_BUS = (detected & REACTOR_BUS) == REACTOR_BUS;
        HAS_REACTOR_NETTY = (detected & REACTOR_NETTY) == REACTOR_NETTY;

    }

    public static boolean hasRxJava1() {
        return RXJAVA1_OBSERVABLE_CONVERTER != null;
    }

    public static boolean hasRxJava1Single() {
        return RXJAVA1_SINGLE_CONVERTER != null;
    }

    public static boolean hasRxJava1Completable() {
        return RXJAVA1_COMPLETABLE_CONVERTER != null;
    }

    public static boolean hasFlowPublisher() {
        return FLOW_PUBLISHER_CONVERTER != null;
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

    public static Publisher<?> toPublisher(Object source) {
        if (source == null) {
            throw new IllegalArgumentException("Cannot convert null sources");
        }
        Class<?> cls = source.getClass();

        if (hasRxJava1()) {
            if (hasRxJava1Single() && RXJAVA1_SINGLE_CONVERTER.canConvert(cls)) {
                return RXJAVA1_SINGLE_CONVERTER.toPublisher(source);
            }
            else if (hasRxJava1Completable() && RXJAVA1_COMPLETABLE_CONVERTER.canConvert(cls)) {
                return RXJAVA1_COMPLETABLE_CONVERTER.toPublisher(source);
            }
            else if (RXJAVA1_OBSERVABLE_CONVERTER.canConvert(cls)) {
                return RXJAVA1_OBSERVABLE_CONVERTER.toPublisher(source);
            }
        }

        if (hasFlowPublisher() && FLOW_PUBLISHER_CONVERTER.canConvert(cls)) {
            return FLOW_PUBLISHER_CONVERTER.toPublisher(source);
        }
        throw new UnsupportedOperationException("Conversion to Publisher from " + cls);
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromPublisher(Publisher<?> source, Class<T> to) {
        if (source == null || to == null) {
            throw new IllegalArgumentException("Cannot convert " + source + " source to " + to + " type");
        }
        if (hasRxJava1()) {
            if (hasRxJava1Single() && RXJAVA1_SINGLE_CONVERTER.canConvert(to)) {
                return (T) RXJAVA1_SINGLE_CONVERTER.fromPublisher(source);
            }
            else if (hasRxJava1Completable() && RXJAVA1_COMPLETABLE_CONVERTER.canConvert(to)) {
                return (T) RXJAVA1_COMPLETABLE_CONVERTER.fromPublisher(source);
            }
            else if (RXJAVA1_OBSERVABLE_CONVERTER.canConvert(to)) {
                return (T) RXJAVA1_OBSERVABLE_CONVERTER.fromPublisher(source);
            }
        }
        if (hasFlowPublisher() && FLOW_PUBLISHER_CONVERTER.canConvert(to)) {
            return (T) FLOW_PUBLISHER_CONVERTER.fromPublisher(source);
        }
        throw new UnsupportedOperationException("Cannot convert " + source.getClass() +
                " source to " + to.getClass() + " type");
    }

    private interface ConverterWrapper {
        Publisher<?> toPublisher(Object o);
        Object fromPublisher(Publisher<?> source);
        boolean canConvert(Class<?> cls);
    }

    @SuppressWarnings("unchecked")
    private static class FlowPublisherConverterWrapper implements ConverterWrapper {
        @Override
        public Publisher<?> toPublisher(Object o) {
            if (o instanceof Flow.Publisher) {
                return FlowPublisherConverter.toPublisher((Flow.Publisher)o);
            }
            throw new IllegalArgumentException("Input parameter must be a Flow.Publisher instance");
        }
        @Override
        public Object fromPublisher(Publisher<?> source) {
            return FlowPublisherConverter.fromPublisher(source);
        }
        @Override
        public boolean canConvert(Class<?> cls) {
            return Flow.Publisher.class.isAssignableFrom(cls);
        }
    }

    @SuppressWarnings("unchecked")
    private static class RxJava1ObservableConverterWrapper implements ConverterWrapper {
        @Override
        public Publisher<?> toPublisher(Object o) {
            if (o instanceof Observable) {
                return RxJava1ObservableConverter.toPublisher((Observable)o);
            }
            throw new IllegalArgumentException("Input parameter must be a Observable instance");
        }
        @Override
        public Object fromPublisher(Publisher<?> source) {
            return RxJava1ObservableConverter.fromPublisher(source);
        }
        @Override
        public boolean canConvert(Class<?> cls) {
            return Observable.class.isAssignableFrom(cls);
        }
    }

    @SuppressWarnings("unchecked")
    private static class RxJava1SingleConverterWrapper implements ConverterWrapper {
        @Override
        public Publisher<?> toPublisher(Object o) {
            if (o instanceof Single) {
                return RxJava1SingleConverter.toPublisher((Single)o);
            }
            throw new IllegalArgumentException("Input parameter must be a Observable instance");
        }
        @Override
        public Object fromPublisher(Publisher<?> source) {
            return RxJava1SingleConverter.fromPublisher(source);
        }
        @Override
        public boolean canConvert(Class<?> cls) {
            return Single.class.isAssignableFrom(cls);
        }
    }

    @SuppressWarnings("unchecked")
    private static class RxJava1CompletableConverterWrapper implements ConverterWrapper {
        @Override
        public Publisher<?> toPublisher(Object o) {
            if (o instanceof Completable) {
                return RxJava1CompletableConverter.toPublisher((Completable)o);
            }
            throw new IllegalArgumentException("Input parameter must be a Observable instance");
        }
        @Override
        public Object fromPublisher(Publisher<?> source) {
            return RxJava1CompletableConverter.fromPublisher(source);
        }

        @Override
        public boolean canConvert(Class<?> cls) {
            return Completable.class.isAssignableFrom(cls);
        }
    }

}
