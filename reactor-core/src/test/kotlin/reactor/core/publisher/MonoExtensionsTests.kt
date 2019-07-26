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

import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert
import org.junit.Test
import org.reactivestreams.Publisher
import reactor.test.StepVerifier
import reactor.test.publisher.TestPublisher
import reactor.test.test
import reactor.test.verifyError
import java.io.IOException
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture

@Suppress("deprecation")
class MonoExtensionsTests {

    @Test
    fun anyToMono() {
        StepVerifier
                .create("foo".toMono())
                .expectNext("foo")
                .verifyComplete()
    }

    @Test
    fun supplierToMono() {
        val supplier: () -> String = { "a" }

        val m = supplier.toMono()

        m.test()
                .expectNext("a")
                .verifyComplete()
    }

    @Test
    fun publisherToMono() {
        //fake naive publisher
        val p: Publisher<String> = Publisher {
            it.onSubscribe(Operators.emptySubscription())
            it.onNext("a")
            it.onNext("b")
            it.onComplete()
        }

        val m = p.toMono()

        assertThat(m).isNotSameAs(p)
        m.test()
                .expectNext("a")
                .verifyComplete()
    }
    @Test
    fun fluxToMono() {
        val f = Flux.range(1, 2)
        val m = f.toMono()

        assertThat(m).isNotSameAs(f)
        m.test()
                .expectNext(1)
                .verifyComplete()
    }

    @Test
    fun monoToMono() {
        val m = Mono.just(2)
        assertThat(m.toMono()).isSameAs(m)
    }

    @Test
    fun completableFutureToMono() {
        val future = CompletableFuture<String>()

        val verifier = StepVerifier.create(future.toMono())
                .expectNext("foo")
                .expectComplete()
        future.complete("foo")
        verifier.verify()
    }

    @Test
    fun nullableCompletableFutureToMonoWithMap() {
        val future = CompletableFuture<String?>()

        val verifier = StepVerifier.create(future.toMono().map { it.toUpperCase() })
            .expectComplete()
        future.complete(null)
        verifier.verify()
    }

    @Test
    fun callableToMono() {
        val callable = Callable { "foo" }
        val verifier = StepVerifier.create(callable.toMono())
                .expectNext("foo")
                .expectComplete()
        verifier.verify()
    }

    @Test
    fun nullableCallableToMonoWithMap() {
        val callable = Callable<String?> { "foo" }
        val verifier = StepVerifier.create(callable.toMono().map { it.toUpperCase() })
            .expectNext("FOO")
            .expectComplete()
        verifier.verify()
    }

    @Test
    fun nullableCallableToEmptyMonoWitMap() {
        val callable = Callable<String?> { null }
        val verifier = StepVerifier.create(callable.toMono().map { it.toUpperCase() })
            .expectComplete()
        verifier.verify()
    }

    @Test
    fun nullableLambdaToEmptyMono() {
        val callable = { null }
        val verifier = StepVerifier.create(callable.toMono())
            .expectComplete()
        verifier.verify()
    }

    @Test
    fun nullableLambdaToEmptyMonoWithMap() {
        @Suppress("USELESS_CAST")
        val callable = { null as String? }
        val verifier = StepVerifier.create(callable.toMono().map { it.toUpperCase() })
            .expectComplete()
        verifier.verify()
    }

    @Test
    fun throwableToMono() {
        StepVerifier.create(IllegalStateException()
                .toMono<Any>())
                .verifyError(IllegalStateException::class)
    }

    @Test
    fun `cast() with generic parameter`() {
        val monoOfAny: Mono<Any> = Mono.just("foo")
        StepVerifier
                .create(monoOfAny.cast<String>())
                .expectNext("foo")
                .verifyComplete()
    }

    @Test
    fun doOnError() {
        val monoOnError: Mono<Any> = IllegalStateException().toMono()
        var invoked = false
        monoOnError.doOnError(IllegalStateException::class) {
            invoked = true
        }.subscribe()
        Assert.assertTrue(invoked)
    }

    @Test
    fun onErrorMap() {
        StepVerifier.create(IOException()
                .toMono<Any>()
                .onErrorMap(IOException::class, ::IllegalStateException))
                .verifyError<IllegalStateException>()
    }

    @Test
    fun `ofType() with generic parameter`() {
        StepVerifier.create("foo".toMono().ofType<String>()).expectNext("foo").verifyComplete()
        StepVerifier.create("foo".toMono().ofType<Int>()).verifyComplete()
    }

    @Test
    fun onErrorResume() {
        val mono = IOException().toMono<String>().onErrorResume(IOException::class) { "foo".toMono() }
        StepVerifier.create(mono)
                .expectNext("foo")
                .verifyComplete()
    }

    @Test
    fun onErrorReturn() {
        StepVerifier.create(IOException()
                .toMono<String>()
                .onErrorReturn(IOException::class, "foo"))
                .expectNext("foo")
                .verifyComplete()
    }

    @Test
    fun `switchIfEmpty with defer execution`() {
        val mono: Mono<String> = "foo"
                .toMono()
                .switchIfEmpty { throw RuntimeException("error which should not happen due to defered execution") }

        StepVerifier
                .create(mono)
                .expectNext("foo")
                .verifyComplete()
    }

    @Test
    fun `whenComplete on an Iterable of void Publishers`() {
        val publishers = Array(3) { TestPublisher.create<Void>() }
        publishers.forEach { it.complete() }
        StepVerifier.create(publishers.asIterable().whenComplete())
                .verifyComplete()
    }

    @Test
    fun `whenComplete on an Iterable of String Publishers`() {
        val publishers = Array(3) { TestPublisher.create<String>() }
        publishers.forEach { it.complete() }
        StepVerifier.create(publishers.asIterable().whenComplete())
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
    fun `zip on an Iterable of Monos with combinator`() {
        StepVerifier.create(listOf("foo1", "foo2", "foo3").map { it.toMono() }.zip { it.joinToString() })
                .expectNext("foo1, foo2, foo3")
                .verifyComplete()
    }

}
