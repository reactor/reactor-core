/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.function


/**
 * Extension for [Tuple2] to work with destructuring declarations.
 *
 * @author DoHyung Kim
 * @since 3.1
 */
operator fun <T> Tuple2<T, *>.component1(): T = t1

/**
 * Extension for [Tuple2] to work with destructuring declarations.
 *
 * @author DoHyung Kim
 * @since 3.1
 */
operator fun <T> Tuple2<*, T>.component2(): T = t2

/**
 * Extension for [Tuple3] to work with destructuring declarations.
 *
 * @author DoHyung Kim
 * @since 3.1
 */
operator fun <T> Tuple3<*, *, T>.component3(): T = t3

/**
 * Extension for [Tuple4] to work with destructuring declarations.
 *
 * @author DoHyung Kim
 * @since 3.1
 */
operator fun <T> Tuple4<*, *, *, T>.component4(): T = t4

/**
 * Extension for [Tuple5] to work with destructuring declarations.
 *
 * @author DoHyung Kim
 * @since 3.1
 */
operator fun <T> Tuple5<*, *, *, *, T>.component5(): T = t5

/**
 * Extension for [Tuple6] to work with destructuring declarations.
 *
 * @author DoHyung Kim
 * @since 3.1
 */
operator fun <T> Tuple6<*, *, *, *, *, T>.component6(): T = t6

/**
 * Extension for [Tuple7] to work with destructuring declarations.
 *
 * @author DoHyung Kim
 * @since 3.1
 */
operator fun <T> Tuple7<*, *, *, *, *, *, T>.component7(): T = t7

/**
 * Extension for [Tuple8] to work with destructuring declarations.
 *
 * @author DoHyung Kim
 * @since 3.1
 */
operator fun <T> Tuple8<*, *, *, *, *, *, *, T>.component8(): T = t8
