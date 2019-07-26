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
import reactor.test.test
import reactor.test.verifyError
import java.io.IOException

@Suppress("deprecation")
class FluxExtensionsTests {

    @Test
    fun `Iterator to Flux`() {
        StepVerifier
                .create(listOf("foo", "bar", "baz").listIterator().toFlux())
                .expectNext("foo", "bar", "baz")
                .verifyComplete()
    }

    @Test
    fun `Iterable to Flux`() {
        StepVerifier
                .create(listOf("foo", "bar", "baz").asIterable().toFlux())
                .expectNext("foo", "bar", "baz")
                .verifyComplete()
    }

    @Test
    fun `Sequence to Flux`() {
        StepVerifier
                .create(listOf("foo", "bar", "baz").asSequence().toFlux())
                .expectNext("foo", "bar", "baz")
                .verifyComplete()
    }

    @Test
    fun `Stream to Flux`() {
        StepVerifier
                .create(listOf("foo", "bar", "baz").stream().toFlux())
                .expectNext("foo", "bar", "baz")
                .verifyComplete()
    }

    @Test
    fun `ByteArray to Flux`() {
        StepVerifier
                .create(byteArrayOf(Byte.MAX_VALUE, Byte.MIN_VALUE, Byte.MAX_VALUE).toFlux())
                .expectNext(Byte.MAX_VALUE, Byte.MIN_VALUE, Byte.MAX_VALUE)
                .verifyComplete()
    }

    @Test
    fun `ShortArray to Flux`() {
        StepVerifier
                .create(shortArrayOf(1, 2, 3).toFlux())
                .expectNext(1, 2, 3)
                .verifyComplete()
    }

    @Test
    fun `IntArray to Flux`() {
        StepVerifier
                .create(intArrayOf(1, 2, 3).toFlux())
                .expectNext(1, 2, 3)
                .verifyComplete()
    }

    @Test
    fun `LongArray to Flux`() {
        StepVerifier
                .create(longArrayOf(1, 2, 3).toFlux())
                .expectNext(1, 2, 3)
                .verifyComplete()
    }

    @Test
    fun `FloatArray to Flux`() {
        StepVerifier
                .create(floatArrayOf(1.0F, 2.0F, 3.0F).toFlux())
                .expectNext(1.0F, 2.0F, 3.0F)
                .verifyComplete()
    }

    @Test
    fun `DoubleArray to Flux`() {
        StepVerifier
                .create(doubleArrayOf(1.0, 2.0, 3.0).toFlux())
                .expectNext(1.0, 2.0, 3.0)
                .verifyComplete()
    }

    @Test
    fun `BooleanArray to Flux`() {
        StepVerifier
                .create(booleanArrayOf(true, false, true).toFlux())
                .expectNext(true, false, true)
                .verifyComplete()
    }


    @Test
    fun `Throwable to Flux`() {
        StepVerifier
                .create(IllegalStateException().toFlux<Any>())
                .verifyError(IllegalStateException::class)
    }

    @Test
    fun `cast() with generic parameter`() {
        val fluxOfAny: Flux<Any> = Flux.just("foo")
        StepVerifier
                .create(fluxOfAny.cast<String>())
                .expectNext("foo").verifyComplete()
    }

    @Test
    fun doOnError() {
        val fluxOnError: Flux<Any> = IllegalStateException().toFlux()
        var invoked = false
        fluxOnError.doOnError(IllegalStateException::class) {
            invoked = true
        }.subscribe()
        Assert.assertTrue(invoked)
    }

    @Test
    fun onErrorMap() {
        StepVerifier
                .create(IOException()
                        .toFlux<Any>()
                        .onErrorMap(IOException::class, ::IllegalStateException))
                .verifyError<IllegalStateException>()
    }

    @Test
    fun `ofType() with generic parameter`() {
        StepVerifier
                .create(arrayOf("foo", 1).toFlux().ofType<String>())
                .expectNext("foo").verifyComplete()
    }

    @Test
    fun onErrorResume() {
        val flux = IOException().toFlux<String>().onErrorResume(IOException::class) { "foo".toMono() }
        StepVerifier
                .create(flux)
                .expectNext("foo")
                .verifyComplete()
    }

    @Test
    fun onErrorReturn() {
        StepVerifier
                .create(IOException()
                    .toFlux<String>()
                    .onErrorReturn(IOException::class, "foo"))
                .expectNext("foo")
                .verifyComplete()
    }

    @Test
    fun publisherToFlux() {
        //fake naive publisher
        val p: Publisher<String> = Publisher {
            it.onSubscribe(Operators.emptySubscription())
            it.onNext("a")
            it.onNext("b")
            it.onComplete()
        }

        val f = p.toFlux()

        f.test()
                .expectNext("a", "b")
                .verifyComplete()

        assertThat(f).isNotSameAs(p)
    }

    @Test
    fun fluxToFlux() {
        val f = Flux.range(1, 2)

        assertThat(f.toFlux()).isSameAs(f)
    }

    @Test
    fun monoToFlux() {
        val m = Mono.just(2)
        val f = m.toFlux()

        assertThat(f).isNotSameAs(m)
        f.test()
                .expectNext(2)
                .verifyComplete()
    }

    @Test
    fun splitFlux() {
        val f = listOf(listOf(1, 2), listOf(3, 4)).toFlux()
        StepVerifier
                .create(f.split())
                .expectNext(1, 2, 3, 4)
                .verifyComplete()
    }

    @Test
    fun `switchIfEmpty with defer execution`() {
        val flux: Flux<Int> = listOf(1, 2, 3)
                .toFlux()
                .switchIfEmpty { throw RuntimeException("error which should not happen due to defered execution") }

        StepVerifier
                .create(flux)
                .expectNext(1, 2, 3)
                .verifyComplete()
    }
}
