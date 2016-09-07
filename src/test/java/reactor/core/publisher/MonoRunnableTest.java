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

import org.junit.Test;
import reactor.core.Fuseable;
import reactor.test.TestSubscriber;

public class MonoRunnableTest {

    @Test(expected = NullPointerException.class)
    public void nullValue() {
        new MonoRunnable(null);
    }

    @Test
    public void normal() {
        TestSubscriber<Void> ts = TestSubscriber.create();

        Mono.fromRunnable(() -> {}).subscribe(ts);

        ts.assertNoValues()
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Void> ts = TestSubscriber.create(0);

        Mono.fromRunnable(() -> {}).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(1);

        ts.assertNoValues()
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void fused() {
        TestSubscriber<Void> ts = TestSubscriber.create();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Mono.fromRunnable(() -> {}).subscribe(ts);
        
        ts.assertFuseableSource()
        .assertFusionMode(Fuseable.SYNC)
        .assertNoValues();
    }
}
