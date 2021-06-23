/*
 * Copyright (c) 2011-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@file:JvmName("MonoWhenFunctionsKt") // TODO Remove in next major version
package reactor.core.publisher

import org.reactivestreams.Publisher

/**
 * Aggregates this [Iterable] of void [Publisher]s into a new [Mono].
 * An alias for a corresponding [Mono.when] to avoid use of `when`, which is a keyword in Kotlin.
 *
 * TODO Move to MonoExtensions.kt in next major version
 *
 * @author DoHyung Kim
 * @author Sebastien Deleuze
 * @since 3.1
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("whenComplete()", "reactor.kotlin.core.publisher.whenComplete"))
fun Iterable<Publisher<*>>.whenComplete(): Mono<Void> = Mono.`when`(this)

/**
 * Merges this [Iterable] of [Mono]s into a new [Mono] by combining them
 * with [combinator].
 *
 * TODO Move to MonoExtensions.kt in next major version
 *
 * @author DoHyung Kim
 * @since 3.1
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("zip(combinator)", "reactor.kotlin.core.publisher.zip"))
@Suppress("UNCHECKED_CAST")
inline fun <T, R> Iterable<Mono<T>>.zip(crossinline combinator: (List<T>) -> R): Mono<R> =
        Mono.zip(this) { combinator(it.asList() as List<T>) }

/**
 * Aggregates the given void [Publisher]s into a new void [Mono].
 * An alias for a corresponding [Mono.when] to avoid use of `when`, which is a keyword in Kotlin.
 *
 * @author DoHyung Kim
 * @author Sebastien Deleuze
 * @since 3.1
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("whenComplete(*sources)", "reactor.kotlin.core.publisher.whenComplete"))
fun whenComplete(vararg sources: Publisher<*>): Mono<Void> = MonoBridges.`when`(sources)

/**
 * Aggregates the given [Mono]s into a new [Mono].
 *
 * @author DoHyung Kim
 * @since 3.1
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("zip(*monos, combinator)", "reactor.kotlin.core.publisher.zip"))
@Suppress("UNCHECKED_CAST")
fun <R> zip(vararg monos: Mono<*>, combinator: (Array<*>) -> R): Mono<R> =
        MonoBridges.zip(combinator, monos)