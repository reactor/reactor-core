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

package reactor.core.publisher.scenarios;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

/**
 * @author Stephane Maldini
 */
public class GuideTests {

	@Test
	public void fluxComposing() throws Exception {
		Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
		    .doOnNext(System.out::println)
		    .filter(color -> !color.equals("orange"))
		    .map(String::toUpperCase)
		    .subscribe(d -> System.out.println("Subscriber to Map: "+d));

		System.out.println("\n");

		Flux<String> flux =
				Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
				    .doOnNext(System.out::println)
				    .filter(color -> !color.equals("orange"));

		flux.map(String::toUpperCase);
		flux.subscribe(d -> System.out.println("Subscriber to Filter: "+d));

		System.out.println("\n");

		Flux<String> source =
				Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
				    .doOnNext(System.out::println)
				    .filter(color -> !color.equals("orange"))
				    .map(String::toUpperCase);

		source.subscribe(d -> System.out.println("Subscriber 1: "+d));
		source.subscribe(d -> System.out.println("Subscriber 2: "+d));

		System.out.println("\n");

		Function<Flux<String>, Flux<String>> filterAndMap =
				f -> f.filter(color -> !color.equals("orange"))
				      .map(String::toUpperCase);

		Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
		    .doOnNext(System.out::println)
		    .transform(filterAndMap)
		    .subscribe(d -> System.out.println("Subscriber to Transformed MapAndFilter: "+d));

		System.out.println("\n");

		AtomicInteger ai = new AtomicInteger();
		filterAndMap = f -> {
			if (ai.incrementAndGet() == 1) {
				return f.filter(color -> !color.equals("orange"))
				        .map(String::toUpperCase);
			}
			return f.filter(color -> !color.equals("purple"))
			        .map(String::toUpperCase);
		};

		Flux<String> composedFlux =
				Flux.fromIterable(Arrays.asList("blue", "green", "orange", "purple"))
				    .doOnNext(System.out::println)
				    .compose(filterAndMap);

		composedFlux.subscribe(d -> System.out.println("Subscriber 1 to Composed MapAndFilter :"+d));
		composedFlux.subscribe(d -> System.out.println("Subscriber 2 to Composed MapAndFilter: "+d));

		System.out.println("\n");

		UnicastProcessor<String> hotSource = UnicastProcessor.create();

		Flux<String> hotFlux = hotSource.doOnNext(System.out::println)
		                                .publish()
		                                .autoConnect();

		hotFlux.subscribe(d -> System.out.println("Subscriber 1 to Hot Source: "+d));

		hotSource.onNext("blue");
		hotSource.onNext("green");

		hotFlux.subscribe(d -> System.out.println("Subscriber 2 to Hot Source: "+d));

		hotSource.onNext("orange");
		hotSource.onNext("purple");
		hotSource.onComplete();

	}

}
