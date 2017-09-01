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

package reactor.util.function;

/**
 * An operation that accepts seven input arguments and returns no result.
 *
 * @param <T1> The type of the first input to the operation
 * @param <T2> The type of the second input to the operation
 * @param <T3> The type of the third input to the operation
 * @param <T4> The type of the fourth input to the operation
 * @param <T5> The type of the fifth input to the operation
 * @param <T6> The type of the sixth input to the operation
 * @param <T7> The type of the seventh input to the operation
 * @author Ben Hale
 */
@FunctionalInterface
public interface Consumer7<T1, T2, T3, T4, T5, T6, T7> {

    /**
     * Performs this operation on the given arguments.
     *
     * @param t1 the first input argument
     * @param t2 the second input argument
     * @param t3 the third input argument
     * @param t4 the fourth input argument
     * @param t5 the fifth input argument
     * @param t6 the sixth input argument
     * @param t7 the seventh input argument
     */
    void accept(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7);

}
