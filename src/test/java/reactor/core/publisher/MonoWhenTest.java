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

import reactor.core.util.function.*;

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
    public void someEmpty() {
        Assert.assertNull(Mono.when(
                Mono.empty(), 
                Mono.delay(Duration.ofMillis(250))
            )
            .block());
    }

    @Test//(timeout = 5000)
    public void all2NonEmpty() {
        Assert.assertEquals(Tuple.of(0L, 0L), 
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
            
            Object[] out = Mono.when(monos).block();
            
            Assert.assertArrayEquals(result, out);
        }
    }
}
