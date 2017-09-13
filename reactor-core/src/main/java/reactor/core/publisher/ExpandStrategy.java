/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

public enum ExpandStrategy {

	/**
	 * Given the hierarchical structure
	 *  A
	 *   - AA
	 *     - aa1
	 *   - AB
	 *     - ab1
	 *   - a1
	 *  B
	 *   - BA
	 *     - ba1
	 *   - BB
	 *     - bb1
	 *   - BC
	 *     - bc1
	 *     - bc2
	 *   - b1
	 *   - b2
	 *
	 * Visits
	 *  A
	 *  AA
	 *  aa1
	 *  AB
	 *  ab1
	 *  a1
	 *  B
	 *  BA
	 *  ba1
	 *  BB
	 *  bb1
	 *  BC
	 *  bc1
	 *  bc2
	 *  b1
	 *  b2
	 */
	DEPTH_FIRST,

	/**
	 * Given the hierarchical structure
	 *  A
	 *   - AA
	 *     - aa1
	 *   - AB
	 *     - ab1
	 *   - a1
	 *  B
	 *   - BA
	 *     - ba1
	 *   - BB
	 *     - bb1
	 *   - BC
	 *     - bc1
	 *     - bc2
	 *   - b1
	 *   - b2
	 *
	 * Visits
	 *  A
	 *  B
	 *  AA
	 *  AB
	 *  a1
	 *  BA
	 *  BB
	 *  BC
	 *  b1
	 *  b2
	 *  aa1
	 *  ab1
	 *  ba1
	 *  bb1
	 *  bc1
	 *  bc2
	 */
	BREADTH_FIRST;

}
