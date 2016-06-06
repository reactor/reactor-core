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

package reactor.core.tuple;

/**
 * Implementations accept a given value and perform work on the arguments.
 *
 * @param <T1> The type of the first value.
 * @param <T2> The type of the second value.
 * @param <T3> The type of the third value.
 * @param <T4> The type of the fourth value.
 * @param <T5> The type of the fifth value.
 * @param <T6> The type of the sixth value.
 * @author Ben Hale
 */
public interface Consumer6<T1, T2, T3, T4, T5, T6> {

    /**
     * Execute the logic of the action, accepting the given parameters.
     *
     * @param t1 The first value in the tuple.
     * @param t2 The second value in the tuple.
     * @param t3 The third value in the tuple.
     * @param t4 The fourth value in the tuple.
     * @param t5 The fifth value in the tuple.
     * @param t6 The sixth value in the tuple.
     */
    void accept(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6);

}
