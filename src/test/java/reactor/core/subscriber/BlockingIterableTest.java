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
package reactor.core.subscriber;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;

public class BlockingIterableTest {

	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(BlockingIterable.class);
		
		ctb.addRef("source", Flux.never());
		ctb.addLong("batchSize", 1, Long.MAX_VALUE);
		ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
		
		ctb.test();
	}*/
	
	@Test(timeout = 1000)
	public void normal() {
		List<Integer> values = new ArrayList<>();
		
		for (Integer i : Flux.fromIterable(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toIterable()) {
			values.add(i);
		}
		
		Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values);
	}

	@Test(timeout = 1000)
	public void empty() {
		List<Integer> values = new ArrayList<>();
		
		for (Integer i : Flux.<Integer>empty().toIterable()) {
			values.add(i);
		}
		
		Assert.assertEquals(Collections.emptyList(), values);
	}
	
	@Test(timeout = 1000, expected = RuntimeException.class)
	public void error() {
		List<Integer> values = new ArrayList<>();
		
		for (Integer i : Flux.<Integer>error(new RuntimeException("forced failure")).toIterable()) {
			values.add(i);
		}
		
		Assert.assertEquals(Collections.emptyList(), values);
	}
}
