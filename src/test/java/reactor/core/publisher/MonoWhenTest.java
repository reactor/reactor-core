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

import java.time.Duration;
import java.util.Arrays;

import org.junit.*;

import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.*;

public class MonoWhenTest {

    @Test(timeout = 5000)
    public void allEmpty() {
        Assert.assertNull(Mono.when(
                Mono.empty(), 
                Mono.delay(Duration.ofMillis(250)).ignoreElement()
            )
            .block());
    }
    @Test(timeout = 5000)
    public void castCheck() {
        Mono<String[]> mono = Mono.when(a ->  Arrays.copyOf(a, a.length, String[]
		        .class), Mono.just("hello"), Mono.just
			    ("world"));
        mono.subscribe(System.out::println);
    }

    @Test(timeout = 5000)
    public void someEmpty() {
        Assert.assertNull(Mono.when(
                Mono.empty(), 
                Mono.delay(Duration.ofMillis(250))
            )
            .block());
    }

    @Test//(timeout = 5000)
    public void all2NonEmpty() {
        Assert.assertEquals(Tuples.of(0L, 0L),
                Mono.when(Mono.delayMillis(150), Mono.delayMillis(250)).block()
        );
    }
    
    @Test//(timeout = 5000)
    public void allNonEmpty() {
        for (int i = 2; i < 7; i++) {
            Long[] result = new Long[i];
            Arrays.fill(result, 0L);
            
            @SuppressWarnings("unchecked")
            Mono<Long>[] monos = new Mono[i];
            for (int j = 0; j < i; j++) {
                monos[j] = Mono.delay(Duration.ofMillis(150 + 50 * j));
            }
            
            Object[] out = Mono.when(a -> a, monos).block();
            
            Assert.assertArrayEquals(result, out);
        }
    }

    @Test
    public void pairWise() {
        Mono<Tuple2<Integer, String>> f = Mono.when(Mono.just(1), Mono.just("test"))
                                              .and(Mono.just("test2"))
                                              .map(t -> Tuples.of(t.getT1()
                                                                   .getT1(),
		                                              t.getT1()
		                                               .getT2() + t.getT2()));
        f.subscribeWith(AssertSubscriber.create())
         .assertValues(Tuples.of(1, "testtest2"))
         .assertComplete();
    }
}
