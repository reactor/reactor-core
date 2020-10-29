/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.test.MockUtils;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxAutoConnectTest {

	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(StreamAutoConnect.class);
		
		ctb.addRef("source", Flux.never().publish());
		ctb.addInt("n", 1, Integer.MAX_VALUE);
		ctb.addRef("cancelSupport", (Consumer<Runnable>)r -> { });
		
		ctb.test();
	}*/
	
	@Test
	public void connectImmediately() {
		Sinks.Many<Integer> e = Sinks.many().multicast().onBackpressureBuffer();

		AtomicReference<Disposable> cancel = new AtomicReference<>();
		
		e.asFlux().publish().autoConnect(0, cancel::set);

		assertThat(cancel).doesNotHaveValue(null);
		assertThat(e.currentSubscriberCount()).as("source subscribed").isPositive();

		cancel.get().dispose();
		assertThat(e.currentSubscriberCount()).as("source subscribed").isZero();
	}

	@Test
	public void connectAfterMany() {
		Sinks.Many<Integer> e = Sinks.many().multicast().onBackpressureBuffer();

		AtomicReference<Disposable> cancel = new AtomicReference<>();
		
		Flux<Integer> p = e.asFlux().publish().autoConnect(2, cancel::set);

		assertThat(cancel).hasValue(null);
		assertThat(e.currentSubscriberCount()).as("source subscribed").isZero();

		p.subscribe(AssertSubscriber.create());

		assertThat(cancel).hasValue(null);
		assertThat(e.currentSubscriberCount()).as("source subscribed").isZero();

		p.subscribe(AssertSubscriber.create());

		assertThat(cancel.get()).isNotNull();
		assertThat(e.currentSubscriberCount()).as("source subscribed").isPositive();

		cancel.get().dispose();
		assertThat(e.currentSubscriberCount()).as("source subscribed").isZero();
	}

	@Test
	public void scanMain() {
		@SuppressWarnings("unchecked")
		ConnectableFlux<String> source = Mockito.mock(MockUtils.TestScannableConnectableFlux.class);
		Mockito.when(source.getPrefetch()).thenReturn(888);
		FluxAutoConnect<String> test = new FluxAutoConnect<>(source, 123, d -> { });

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(888);
		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(123);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
