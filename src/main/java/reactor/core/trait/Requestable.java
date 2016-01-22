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

package reactor.core.trait;

/**
 * A request aware component
 */
public interface Requestable {

	/**
	 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription} request needs. This
	 * is the maximum in-flight data allowed to transit to this elements.
	 *
	 * @return long capacity
	 */
	long requestedFromDownstream();
}
