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
package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.Disposable;
import reactor.core.Scannable;
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
		EmitterProcessor<Integer> e = EmitterProcessor.create();

		AtomicReference<Disposable> cancel = new AtomicReference<>();
		
		e.publish().autoConnect(0, cancel::set);
		
		Assert.assertNotNull(cancel.get());
		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		cancel.get().dispose();
		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
	}

	@Test
	public void connectAfterMany() {
		EmitterProcessor<Integer> e = EmitterProcessor.create();

		AtomicReference<Disposable> cancel = new AtomicReference<>();
		
		Flux<Integer> p = e.publish().autoConnect(2, cancel::set);
		
		Assert.assertNull(cancel.get());
		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
		
		p.subscribe(AssertSubscriber.create());
		
		Assert.assertNull(cancel.get());
		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		p.subscribe(AssertSubscriber.create());

		Assert.assertNotNull(cancel.get());
		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);
		
		cancel.get().dispose();
		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
	}

	@Test
	public void scanMain() {
		ConnectableFlux<String> source = Mockito.mock(ConnectableFlux.class);
		Mockito.when(source.getPrefetch()).thenReturn(888);
		FluxAutoConnect<String> test = new FluxAutoConnect<>(source, 123, d -> { });

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(888);
//		assertThat(test.scan(Scannable.IntAttr.CAPACITY)).isEqualTo(123);
		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(source);
	}
}
