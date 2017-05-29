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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxJustTest {

    @Test(expected = NullPointerException.class)
    public void nullValue() {
        Flux.just((Integer)null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void valueSame() throws Exception {
        Assert.assertSame(1, ((Callable<Integer>)Flux.just(1)).call());
    }

    @Test
    public void normal() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        Flux.just(1).subscribe(ts);

        ts.assertValues(1)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

        Flux.just(1).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(1);

        ts.assertValues(1)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void fused() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        ts.requestedFusionMode(Fuseable.ANY);

        Flux.just(1).subscribe(ts);

        ts.assertFuseableSource()
        .assertFusionMode(Fuseable.SYNC)
        .assertValues(1);
    }

    @Test
    public void fluxInitialValueAvailableImmediately() {
        Flux<String> stream = Flux.just("test");
        AtomicReference<String> value = new AtomicReference<>();
        stream.subscribe(value::set);
        assertThat(value.get()).isEqualTo("test");
    }

	@Test
	public void scanSubscription() {
		Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, sub -> sub.request(100));
		FluxJust.WeakScalarSubscription<Integer> test = new FluxJust.WeakScalarSubscription<>(1, actual);

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}
}
