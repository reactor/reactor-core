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

package reactor.core.publisher

import org.junit.Test
import reactor.test.StepVerifier
import reactor.test.publisher.TestPublisher
import reactor.util.function.Tuples

class MonoWhenFunctionsTests {

    @Test
    fun `whenComplete with two Monos`() {
        StepVerifier.create(Mono.zip("foo1".toMono(), "foo2".toMono()))
                .expectNext(Tuples.of("foo1", "foo2"))
                .verifyComplete()
    }

    @Test
    fun `whenComplete with two Monos and combinator`() {
        StepVerifier.create(Mono.zip("foo1".toMono(), "foo2".toMono()) { a, b -> Pair(a, b) })
                .expectNext(Pair("foo1", "foo2"))
                .verifyComplete()
    }

    @Test
    fun `whenComplete with three Monos`() {
        StepVerifier.create(Mono.zip("foo1".toMono(), "foo2".toMono(), "foo3".toMono()))
                .expectNext(Tuples.of("foo1", "foo2", "foo3"))
                .verifyComplete()
    }

    @Test
    fun `whenComplete with four Monos`() {
        StepVerifier.create(Mono.zip("foo1".toMono(), "foo2".toMono(), "foo3".toMono(), "foo4".toMono()))
                .expectNext(Tuples.of("foo1", "foo2", "foo3", "foo4"))
                .verifyComplete()
    }

    @Test
    fun `whenComplete with five Monos`() {
        StepVerifier.create(Mono.zip("foo1".toMono(), "foo2".toMono(), "foo3".toMono(), "foo4".toMono(), "foo5".toMono()))
                .expectNext(Tuples.of("foo1", "foo2", "foo3", "foo4", "foo5"))
                .verifyComplete()
    }

    @Test
    fun `whenComplete with six Monos`() {
        StepVerifier.create(Mono.zip("foo1".toMono(), "foo2".toMono(), "foo3".toMono(),
                "foo4".toMono(), "foo5".toMono(), "foo6".toMono()))
                .expectNext(Tuples.of("foo1", "foo2", "foo3", "foo4", "foo5", "foo6"))
                .verifyComplete()
    }

    @Test
    fun `whenComplete with an Iterable of Mono + and a combinator`() {
        StepVerifier.create(listOf("foo1".toMono(), "foo2".toMono(), "foo3".toMono())
                .zip { it.reduce { acc, s -> acc + s }})
                .expectNext("foo1foo2foo3")
                .verifyComplete()
    }

    @Test
    fun `zip with an Iterable of Mono + and a combinator`() {
        StepVerifier.create(listOf("foo1".toMono(), "foo2".toMono(), "foo3".toMono())
                .zip { it.reduce { acc, s -> acc + s }})
                .expectNext("foo1foo2foo3")
                .verifyComplete()
    }

    @Test
    fun `whenComplete on an Iterable of void Publishers`() {
        val publishers = Array(3, { TestPublisher.create<Void>() })
        publishers.forEach { it.complete() }
        StepVerifier.create(publishers.asIterable().whenComplete())
                .verifyComplete()
    }

    @Test
    fun `whenComplete on an Iterable of Monos with combinator`() {
        StepVerifier.create(listOf("foo1", "foo2", "foo3").map { it.toMono() }.zip { it.joinToString() })
                .expectNext("foo1, foo2, foo3")
                .verifyComplete()
    }

    @Test
    fun `zip on an Iterable of Monos with combinator`() {
        StepVerifier.create(listOf("foo1", "foo2", "foo3").map { it.toMono() }.zip { it.joinToString() })
                .expectNext("foo1, foo2, foo3")
                .verifyComplete()
    }

    @Test
    fun `whenComplete with void Publishers`() {
        val publishers = Array(3, { TestPublisher.create<Void>() })
        publishers.forEach { it.complete() }
        StepVerifier.create(whenComplete(*publishers))
                .verifyComplete()
    }

    @Test
    fun `whenComplete with Monos and combinator`() {
        StepVerifier.create(zip("foo1".toMono(), "foo2".toMono(), "foo3".toMono()) { it.joinToString() })
                .expectNext("foo1, foo2, foo3")
                .verifyComplete()
    }

    @Test
    fun `zip with Monos and combinator`() {
        StepVerifier.create(zip("foo1".toMono(), "foo2".toMono(), "foo3".toMono()) { it.joinToString() })
                .expectNext("foo1, foo2, foo3")
                .verifyComplete()
    }

}
