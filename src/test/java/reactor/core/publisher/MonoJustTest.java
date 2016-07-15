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

import org.junit.Assert;
import org.junit.Test;
import reactor.core.Fuseable;
import reactor.test.TestSubscriber;

public class MonoJustTest {

    @Test(expected = NullPointerException.class)
    public void nullValue() {
        new MonoJust<Integer>(null);
    }

    @Test
    public void valueSame() {
        Assert.assertSame(1, new MonoJust<>(1).call());
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = TestSubscriber.create();

        Mono.just(1).subscribe(ts);

        ts.assertValues(1)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = TestSubscriber.create(0);

        Mono.just(1).subscribe(ts);

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
        TestSubscriber<Integer> ts = TestSubscriber.create();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Mono.just(1).subscribe(ts);
        
        ts.assertFuseableSource()
        .assertFusionMode(Fuseable.SYNC)
        .assertValues(1);
    }
}
