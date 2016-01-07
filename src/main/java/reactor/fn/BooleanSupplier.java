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

package reactor.fn;

/**
 * Implementations of this class supply the caller with a boolean. The provided boolean can be evaluated each call to
 * {@code getAsBoolean()} or can be created in some other way.
 *
 * @author Stephane Maldini
 */
public interface BooleanSupplier {

	/**
	 * Get a primitive boolean.
	 *
	 * @return A boolean.
	 */
	boolean getAsBoolean();

}
