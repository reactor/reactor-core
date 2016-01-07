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
package reactor.core.error;

/**
 * an error that should stop producing more data
 *
 * @author Stephane Maldini
 * @since 2.0.2
 */
public class ReactorFatalException extends RuntimeException {
	public static final ReactorFatalException INSTANCE = new ReactorFatalException("Uncaught exception");

	public static ReactorFatalException instance() {
		return INSTANCE;
	}

	public static ReactorFatalException create(Throwable root) {
		if(ReactorFatalException.class.isAssignableFrom(root.getClass())){
			return (ReactorFatalException)root;
		}
		return new ReactorFatalException(root);
	}

	public ReactorFatalException(String message) {
		super(message);
	}

	private ReactorFatalException(Throwable root) {
		super(root);
	}

}
