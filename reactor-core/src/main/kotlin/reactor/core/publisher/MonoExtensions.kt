/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Publisher
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.function.Supplier
import kotlin.reflect.KClass

/**
 * Extension to convert any [Publisher] of [T] to a [Mono] that only emits its first
 * element.
 *
 * Note this extension doesn't make much sense on a [Mono] but it won't be converted so it
 * doesn't hurt.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Publisher<T>.toMono(): Mono<T> = Mono.from(this)

/**
 * Extension to convert any [Supplier] of [T] to a [Mono] that emits supplied element.
 *
 * @author Sergio Dos Santos
 */
fun <T> (() -> T?).toMono(): Mono<T> = Mono.fromSupplier(this)

/**
 * Extension for transforming an object to a [Mono].
 *
 * @author Sebastien Deleuze
 * @since 3.1
 */
fun <T : Any> T.toMono(): Mono<T> = Mono.just(this)

/**
 * Extension for transforming an [CompletableFuture] to a [Mono].
 *
 * @author Sebastien Deleuze
 * @since 3.1
 */
fun <T> CompletableFuture<out T?>.toMono(): Mono<T> = Mono.fromFuture(this)

/**
 * Extension for transforming an [Callable] to a [Mono].
 *
 * @author Sebastien Deleuze
 * @since 3.1
 */
fun <T> Callable<T?>.toMono(): Mono<T> = Mono.fromCallable(this::call)

/**
 * Extension for transforming an exception to a [Mono] that completes with the specified error.
 *
 * @author Sebastien Deleuze
 * @since 3.1
 */
fun <T> Throwable.toMono(): Mono<T> = Mono.error(this)

/**
 * Extension for [Mono.cast] providing a `cast<Foo>()` variant.
 *
 * @author Sebastien
 * @since 3.1
 */
inline fun <reified T : Any> Mono<*>.cast(): Mono<T> = cast(T::class.java)

/**
 * Extension for [Mono.doOnError] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 3.1
 */
fun <T, E : Throwable> Mono<T>.doOnError(exceptionType: KClass<E>, onError: (E) -> Unit): Mono<T> =
        doOnError(exceptionType.java) { onError(it) }

/**
 * Extension for [Mono.onErrorMap] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 3.1
 */
fun <T, E : Throwable> Mono<T>.onErrorMap(exceptionType: KClass<E>, mapper: (E) -> Throwable): Mono<T> =
        onErrorMap(exceptionType.java) { mapper(it) }

/**
 * Extension for [Mono.ofType] providing a `ofType<Foo>()` variant.
 *
 * @author Sebastien Deleuze
 * @since 3.1
 */
inline fun <reified T : Any> Mono<*>.ofType(): Mono<T> = ofType(T::class.java)

/**
 * Extension for [Mono.onErrorResume] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 3.1
 */
fun <T : Any, E : Throwable> Mono<T>.onErrorResume(exceptionType: KClass<E>, fallback: (E) -> Mono<T>): Mono<T> =
        onErrorResume(exceptionType.java) { fallback(it) }

/**
 * Extension for [Mono.onErrorReturn] providing a [KClass] based variant.
 *
 * @author Sebastien Deleuze
 * @since 3.1
 */
fun <T : Any, E : Throwable> Mono<T>.onErrorReturn(exceptionType: KClass<E>, value: T): Mono<T> =
        onErrorReturn(exceptionType.java, value)

/**
 * Extension for [Mono.switchIfEmpty] accepting a function providing a Mono. This allows having a deferred execution with
 * the [switchIfEmpty] operator
 *
 * @author Kevin Davin
 * @since 3.2
 */
fun <T> Mono<T>.switchIfEmpty(s: () -> Mono<T>): Mono<T> = this.switchIfEmpty(Mono.defer { s() })

/**
 * Aggregates this [Iterable] of void [Publisher]s into a new [Mono].
 * An alias for a corresponding [Mono.when] to avoid use of `when`, which is a keyword in Kotlin.
 *
 * @author DoHyung Kim
 * @author Sebastien Deleuze
 * @since 3.1
 */
fun Iterable<Publisher<*>>.whenComplete(): Mono<Void> = Mono.`when`(this)

/**
 * Merges this [Iterable] of [Mono]s into a new [Mono] by combining them
 * with [combinator].
 *
 * @author DoHyung Kim
 * @since 3.1
 */
@Suppress("UNCHECKED_CAST")
inline fun <T, R> Iterable<Mono<T>>.zip(crossinline combinator: (List<T>) -> R): Mono<R> =
        Mono.zip(this) { combinator(it.asList() as List<T>) }
