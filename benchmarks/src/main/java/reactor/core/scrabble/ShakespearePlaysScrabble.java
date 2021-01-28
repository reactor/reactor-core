package reactor.core.scrabble;

/*
 * Copyright (C) 2019 José Paumard
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Embedded from @akarnokd repository : https://github.com/akarnokd/akarnokd-misc/blob/master/src/jmh/java/hu/akarnokd/comparison/scrabble/ShakespearePlaysScrabble.java
 */
@State(Scope.Benchmark)
public class ShakespearePlaysScrabble {

	public static final int[] letterScores             = {
			// a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p,  q, r, s, t, u, v, w, x, y,  z
			1, 3, 3, 2, 1, 4, 2, 4, 1, 8, 5, 1, 3, 1, 1, 3, 10, 1, 1, 1, 1, 4, 4, 8, 4, 10};
	public static final int[] scrabbleAvailableLetters = {
			// a, b, c, d,  e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z
			9, 2, 2, 1, 12, 2, 3, 2, 9, 1, 1, 4, 2, 6, 8, 2, 1, 6, 4, 6, 4, 2, 2, 1, 2, 1};

	public Set<String> scrabbleWords;
	public Set<String> shakespeareWords;

	@Setup
	public void init() {
		scrabbleWords = read("ospd.txt.gz");
		shakespeareWords = read("words.shakespeare.txt.gz");
	}

	interface LongWrapper {

		long get();

		default LongWrapper incAndSet() {
			return () -> get() + 1L;
		}

		LongWrapper zero = () -> 0;
	}

	static class MutableLong {

		long value;

		long get() {
			return value;
		}

		MutableLong incAndSet() {
			value++;
			return this;
		}
	}

	static Set<String> read(String resourceName) {

		try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(ShakespearePlaysScrabble.class.getClassLoader()
		                                                                                                                    .getResourceAsStream(
				                                                                                                                    resourceName))))

		) {
			return br.lines()
			         .map(String::toLowerCase)
			         .collect(Collectors.toSet());
		}
		catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	static <T> Iterable<T> iterableOf(Spliterator<T> spliterator) {
		return () -> Spliterators.iterator(spliterator);
	}
}