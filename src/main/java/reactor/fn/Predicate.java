/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.fn;

/**
 * Determines if the input object matches some criteria.
 *
 * @param <T> the type of object that the predicate can test
 * @author Jon Brisbin
 */
public interface Predicate<T> {

	/**
	 * Returns {@literal true} if the input object matches some criteria.
	 *
	 * @param t The input object.
	 * @return {@literal true} if the criteria matches, {@literal false} otherwise.
	 */
	boolean test(T t);

}
