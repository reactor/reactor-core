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

package reactor.core.publisher;

import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Consumer;
import java.util.logging.Level;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Stephane Maldini
 */
public class OperatorsTest {

	void simpleFlux(){
		Flux.just(1)
		    .map(d -> d + 1)
		    .doOnNext(d -> {throw new RuntimeException("test");})
		    .collectList()
		    .otherwiseReturn(Collections.singletonList(2))
		    .block();
	}

	@Test
	public void verboseExtension() {
		Queue<String> q = new LinkedTransferQueue<>();

		Operators.enableAssemblyStacktrace();
		Operators.setOnAssemblyHook(p -> new Operators.SignalObserver<Object>() {
					@Override
					public Consumer<Object> onNextCall() {
						return d -> q.offer(p.toString()+ ": "+d);
					}
					@Override
					public Consumer<Throwable> onErrorCall() {
						return d -> q.offer(p.toString()+ "! "+(d.getSuppressed()
								.length != 0));
					}
				});

		simpleFlux();

		Assert.assertTrue(Operators.isAssemblyStacktraceEnabled());
		Assert.assertArrayEquals(q.toArray(), new String[]{
				"Just: 1",
				"{ operator : \"MapFuseable\" }: 2",
				"{ operator : \"PeekFuseable\" }! false",
				"{ operator : \"CollectList\" }! true",
				"Just: [2]",
				"{ operator : \"Otherwise\" }: [2]"
		});

		q.clear();

		Operators.disableAssemblyStacktrace();

		simpleFlux();

		Assert.assertFalse(Operators.isAssemblyStacktraceEnabled());
		Assert.assertArrayEquals(q.toArray(), new String[]{
				"Just: 1",
				"{ operator : \"MapFuseable\" }: 2",
				"{ operator : \"PeekFuseable\" }! false",
				"{ operator : \"CollectList\" }! false",
				"Just: [2]",
				"{ operator : \"Otherwise\" }: [2]"
		});


		q.clear();

		Operators.setOnAssemblyHook(p -> Operators.signalLogger(p, "", Level.INFO));

		simpleFlux();

		Assert.assertFalse(Operators.isAssemblyStacktraceEnabled());
		Assert.assertArrayEquals(q.toArray(), new String[0]);

		q.clear();

		Operators.resetOnAssemblyHook();

		simpleFlux();

		Assert.assertFalse(Operators.isAssemblyStacktraceEnabled());
		Assert.assertArrayEquals(q.toArray(), new String[0]);
	}

}
