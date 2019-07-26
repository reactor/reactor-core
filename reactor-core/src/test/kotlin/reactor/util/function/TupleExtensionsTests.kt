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

import org.junit.Assert.assertEquals
import org.junit.Test

object O1; object O2; object O3; object O4
object O5; object O6; object O7; object O8

@Suppress("deprecation")
class TupleDestructuringTests {

    @Test
    fun destructureTuple2() {
        val (t1, t2) = Tuples.of(O1, O2)
        assertEquals(t1, O1)
        assertEquals(t2, O2)
    }

    @Test
    fun destructureTuple3() {
        val (t1, t2, t3) = Tuples.of(O1, O2, O3)
        assertEquals(t1, O1)
        assertEquals(t2, O2)
        assertEquals(t3, O3)
    }

    @Test
    fun destructureTuple4() {
        val (t1, t2, t3, t4) = Tuples.of(O1, O2, O3, O4)
        assertEquals(t1, O1)
        assertEquals(t2, O2)
        assertEquals(t3, O3)
        assertEquals(t4, O4)
    }

    @Test
    fun destructureTuple5() {
        val (t1, t2, t3, t4, t5) = Tuples.of(O1, O2, O3, O4, O5)
        assertEquals(t1, O1)
        assertEquals(t2, O2)
        assertEquals(t3, O3)
        assertEquals(t4, O4)
        assertEquals(t5, O5)
    }

    @Test
    fun destructureTuple6() {
        val (t1, t2, t3, t4, t5, t6) = Tuples.of(O1, O2, O3, O4, O5, O6)
        assertEquals(t1, O1)
        assertEquals(t2, O2)
        assertEquals(t3, O3)
        assertEquals(t4, O4)
        assertEquals(t5, O5)
        assertEquals(t6, O6)
    }

    @Test
    fun destructureTuple7() {
        val (t1, t2, t3, t4, t5, t6, t7) = Tuples.of(O1, O2, O3, O4, O5, O6, O7)
        assertEquals(t1, O1)
        assertEquals(t2, O2)
        assertEquals(t3, O3)
        assertEquals(t4, O4)
        assertEquals(t5, O5)
        assertEquals(t6, O6)
        assertEquals(t7, O7)
    }

    @Test
    fun destructureTuple8() {
        val (t1, t2, t3, t4, t5, t6, t7, t8) = Tuples.of(O1, O2, O3, O4, O5, O6, O7, O8)
        assertEquals(t1, O1)
        assertEquals(t2, O2)
        assertEquals(t3, O3)
        assertEquals(t4, O4)
        assertEquals(t5, O5)
        assertEquals(t6, O6)
        assertEquals(t7, O7)
        assertEquals(t8, O8)
    }
}
