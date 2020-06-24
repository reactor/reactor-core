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
package reactor.util.stats;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ServiceLoader;

import reactor.util.Logger;
import reactor.util.Loggers;

public class Stats {

	static Logger logger = Loggers.getLogger(Stats.class);

	static StatsReporter defaultStatsReporter;

	static StatsMarker[] statsMarkers;

	static {
		loadDefaultStatsReporter();
		loadMarkers();
	}

	static void loadDefaultStatsReporter() {
		String className = System.getProperty("reactor.trace.operatorStatsReporter");

		if (className != null) {
			className = className.trim();

			if (!className.isEmpty()) {
				try {
					Class<?> aClass = Stats.class.getClassLoader()
					                             .loadClass(className);
					Constructor<?> constructor = aClass.getConstructor();
					defaultStatsReporter = (StatsReporter) constructor.newInstance();
					return;
				} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
					logger.error("Custom StatsPrinter instantiation has failed", e);
				}
			}
		}

		defaultStatsReporter = LoggerStatsReporter.INSTANCE;
	}

	static void loadMarkers() {
		ServiceLoader<StatsMarker> statsMarkerServiceLoader =
				ServiceLoader.load(StatsMarker.class);
		ArrayList<StatsMarker> statsMarkerArrayList = new ArrayList<>();
		for (StatsMarker statsMarker : statsMarkerServiceLoader) {
			statsMarkerArrayList.add(statsMarker);
		}

		statsMarkers = statsMarkerArrayList.toArray(new StatsMarker[0]);
	}

	public static StatsReporter getStatsReporter() {
		return defaultStatsReporter;
	}

	public static StatsMarker[] getStatsMarkers() {
		return statsMarkers;
	}

	public static void setStatsReporter(StatsReporter statsReporter) {
		defaultStatsReporter = statsReporter;
	}

	public static void useLoggerStatsReporter() {
		defaultStatsReporter = LoggerStatsReporter.INSTANCE;
	}

	public static void addMarker(StatsMarker statsMarker) {
		StatsMarker[] newStatsMarkers = Arrays.copyOf(statsMarkers, statsMarkers.length + 1);

		newStatsMarkers[statsMarkers.length] = statsMarker;
		statsMarkers = newStatsMarkers;
	}
}
