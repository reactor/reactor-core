/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import reactor.test.subscriber.AssertSubscriber;

public class MonoStreamCollectorTest {

    @Test
    public void collectToList() {
        Mono<List<Integer>> source = Flux.range(1, 5).collect(Collectors.toList());

        for (int i = 0; i < 5; i++) {
            AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();
            source.subscribe(ts);
    
            ts.assertValues(Arrays.asList(1, 2, 3, 4, 5))
            .assertNoError()
            .assertComplete();
        }
    }

    @Test
    public void collectToSet() {
        Mono<Set<Integer>> source = Flux.just(1).repeat(5).collect(Collectors.toSet());

        for (int i = 0; i < 5; i++) {
            AssertSubscriber<Set<Integer>> ts = AssertSubscriber.create();
            source.subscribe(ts);
    
            ts.assertValues(Collections.singleton(1))
            .assertNoError()
            .assertComplete();
        }
    }

}
