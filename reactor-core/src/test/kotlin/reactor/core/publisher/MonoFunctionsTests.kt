/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher

import org.junit.Test
import reactor.test.StepVerifier
import reactor.test.publisher.TestPublisher

@Suppress("deprecation")
class MonoFunctionsTests {

    @Test
    fun `whenComplete with void Publishers`() {
        val publishers = Array(3) { TestPublisher.create<Void>() }
        publishers.forEach { it.complete() }
        StepVerifier.create(whenComplete(*publishers))
                .verifyComplete()
    }

    @Test
    fun `whenComplete with two Monos`() {
        StepVerifier.create(whenComplete("foo1".toMono(), "foo2".toMono()))
                .verifyComplete()
    }

    @Test
    fun `zip with Monos and combinator`() {
        val mono = zip("foo1".toMono(), "foo2".toMono(), "foo3".toMono()) { it.joinToString() }
        StepVerifier.create(mono)
                .expectNext("foo1, foo2, foo3")
                .verifyComplete()
    }

}
