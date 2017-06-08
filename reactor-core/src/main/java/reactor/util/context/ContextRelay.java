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

package reactor.util.context;

/**
 * A {@link Context} passing component able to fetch or propagate an arbitrary context
 * into dependent components such as a chain of operators.
 *
 * @author Stephane Maldini
 * @since 3.1.0
 */
public interface ContextRelay {

	/**
	 * Try pulling a {@link Context} from the given reference if of
	 * {@link ContextRelay} type.
	 *
	 * @param o the reference to push to if instance of {@link ContextRelay}
	 * @return the pulled {@link Context} or {@link Context#empty()}
	 */
	@SuppressWarnings("unchecked")
	static Context getOrEmpty(Object o) {
		if(o instanceof ContextRelay){
			return ((ContextRelay)o).currentContext();
		}
		return Context.empty();
	}

	/**
	 * Try pushing the passed {@link Context} to the given reference if of
	 * {@link ContextRelay} type.
	 *
	 * @param o the reference to push to if instance of {@link ContextRelay}
	 * @param c the {@link Context} to push
	 */
	@SuppressWarnings("unchecked")
	static void set(Object o, Context c) {
		if (o != Context.empty() && o instanceof ContextRelay) {
			((ContextRelay) o).onContext(c);
		}
	}

	/**
	 * Synchronously push a {@link Context} to dependent components which can include
	 * downstream operators after subscribing or terminal
	 * {@link org.reactivestreams.Subscriber}.
	 *
	 * @param context a new {@link Context} to propagate
	 */
	default void onContext(Context context) {
	}

	/**
	 * Request a {@link Context} from dependent components which can include downstream
	 * operators during subscribing or a terminal {@link org.reactivestreams.Subscriber}.
	 *
	 * @return a resolved context or {@link Context#empty()}
	 */
	default Context currentContext(){
		return Context.empty();
	}
}
