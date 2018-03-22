/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.debug;

/**
 * Utilities around manipulating stack traces and displaying assembly traces.
 *
 * @author Simon Baslé
 */
public class Traces {

	/**
	 * Strip an operator name of various prefixes and suffixes.
	 * @param name the operator name, usually simpleClassName or fully-qualified classname.
	 * @return the stripped operator name
	 */
	public static final String stripOperatorName(String name) {
		if (name.contains("@") && name.contains("$")) {
			name = name.substring(0, name.indexOf('$'));
			name = name.substring(name.lastIndexOf('.') + 1);
		}
		String stripped = name
				.replaceAll("Parallel|Flux|Mono|Publisher|Subscriber", "")
				.replaceAll("Fuseable|Operator|Conditional", "");

		if(stripped.length() > 0) {
			return stripped.substring(0, 1).toLowerCase() + stripped.substring(1);
		}
		return stripped;
	}

}
