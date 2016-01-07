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
package reactor.core.support;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import org.junit.Test;

/**
 * @author Stephane Maldini
 */
public class LogTests {

	static Logger log = Logger.getLogger(LogTests.class);

	@Test
	public void testExtension() throws Exception {
		AtomicInteger count = new AtomicInteger();

		Logger.Extension extension = (c, level, msg, args) -> {
			if(level == Level.FINEST) {
				System.out.println("extension");
				count.incrementAndGet();
			}
		};

		log.trace("test");

		Logger.enableExtension(extension);

		log.trace("test");
		log.trace("test");
		log.info("test");
		log.debug("test");
		log.debug("test");

		Logger.disableExtension(extension);

		log.trace("test");

		Assert.isTrue(count.get() == 2, "Extension should have been used by 2 traces only");
	}

}