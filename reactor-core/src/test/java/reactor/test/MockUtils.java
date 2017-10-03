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

package reactor.test;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.ConnectableFlux;

/**
 * Test utilities that helps with mocking.
 *
 * @author Simon Baslé
 */
public class MockUtils {

	/**
	 * An abstract class that can be used to mock a {@link Scannable} {@link ConnectableFlux}.
	 */
	public static abstract class TestScannableConnectableFlux<T>
			extends ConnectableFlux<T>
			implements Scannable { }

	/**
	 * An interface that can be used to mock a {@link Scannable}
	 * {@link reactor.core.Fuseable.ConditionalSubscriber}.
	 */
	public interface TestScannableConditionalSubscriber<T>
			extends Fuseable.ConditionalSubscriber<T>,
			        Scannable { }

}
