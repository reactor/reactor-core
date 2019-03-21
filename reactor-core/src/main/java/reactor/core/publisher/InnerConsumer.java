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
package reactor.core.publisher;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

/**
 *
 * A {@link InnerConsumer} is a {@link Scannable} {@link CoreSubscriber}.
 *
 * @param <I> input operator produced type
 *
 * @author Stephane Maldini
 */
interface InnerConsumer<I>
		extends CoreSubscriber<I>, Scannable {

	@Override
	default String stepName() {
		// /!\ this code is duplicated in `Scannable#stepName` in order to use toString instead of simple class name

		/*
		 * Strip an operator name of various prefixes and suffixes.
		 * @param name the operator name, usually simpleClassName or fully-qualified classname.
		 * @return the stripped operator name
		 */
		String name = getClass().getSimpleName();
		if (name.contains("@") && name.contains("$")) {
			name = name
				.substring(0, name.indexOf('$'))
				.substring(name.lastIndexOf('.') + 1);
		}
		String stripped = OPERATOR_NAME_UNRELATED_WORDS_PATTERN
			.matcher(name)
			.replaceAll("");

		if (!stripped.isEmpty()) {
			return stripped.substring(0, 1).toLowerCase() + stripped.substring(1);
		}
		return stripped;
	}
}
