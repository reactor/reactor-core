package reactor.util.stats;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple8;
import reactor.util.function.Tuples;

final class LoggerStatsReporter implements StatsReporter {

	static final LoggerStatsReporter INSTANCE = new LoggerStatsReporter();

	static final Logger logger = Loggers.getLogger(LoggerStatsReporter.class);

	static final String                PROBE_CALL_SITE_GLUE = " ↳ ";
	static final String                CALL_SITE_GLUE       = " ⇢ ";
	static final Map<TimeUnit, String> TIME_UNIT_SYMBOLS    =
			new EnumMap<TimeUnit, String>(TimeUnit.class) {
				{
					put(TimeUnit.NANOSECONDS, "ns");
					put(TimeUnit.MICROSECONDS, "μs");
					put(TimeUnit.MILLISECONDS, "ms");
					put(TimeUnit.SECONDS, " s");
				}
			};

	private LoggerStatsReporter() {}

	@Override
	public void report(StatsNode<?> statsNode, StatsMarker... markers) {
		logger.info("The following stats are collected:\n" + buildReport(statsNode, markers));
	}

	static String buildReport(StatsNode<?> statsNode, StatsMarker... markers) {
		int minAvgProcessingTimeWidth = minAvgProcessingTimeWidth(statsNode);
		TimeUnit timeUnit = TimeUnit.NANOSECONDS;
		TimeUnit[] timeUnitValues = TimeUnit.values();
		int k = 0;
		while (minAvgProcessingTimeWidth > 3 && k < 3) {
			k++;
			minAvgProcessingTimeWidth -= 3;
			timeUnit = timeUnitValues[k];
		}


		return buildReport(statsNode, "  \t", timeUnit, -1, markers);
	}


	static int minAvgProcessingTimeWidth(StatsNode<?> statsNode) {
		int minAvgProcessingTimeWidth = Integer.MAX_VALUE;

		while (statsNode != null) {
			long value = avgProcessingTime(statsNode);
			String avgProcessingTime = String.valueOf(value);

			int length = avgProcessingTime.length();
			if (length < minAvgProcessingTimeWidth && value != 0L) {
				minAvgProcessingTimeWidth = length;
			}

			for (StatsNode<?> innerStatsNode : statsNode.innerNodes) {
				length = minAvgProcessingTimeWidth(innerStatsNode);

				if (length < minAvgProcessingTimeWidth) {
					minAvgProcessingTimeWidth = length;
				}
			}

			statsNode = statsNode.upstreamNode;
		}

		return minAvgProcessingTimeWidth;
	}


	static String requested(StatsNode<?> node) {
		return node.requested == Long.MAX_VALUE ? "∞" : String.valueOf(node.requested);
	}

	static String produced(StatsNode<?> node) {
		return String.valueOf(node.produced);
	}

	static long avgProcessingTime(StatsNode<?> node) {
		return node.produced != 0 ? node.totalSumProcessingTimeInNanos / node.produced : 0;
	}

	static String lastObservedSignal(StatsNode<?> node) {
		return node.lastObservedSignal.toString();
	}

	static String state(StatsNode<?> node) {
		return node.cancelled
				? "canceled"
				: node.done
				? node.error == null
				? "completed"
				: "errored"
				: "none";
	}


	static String buildReport(StatsNode<?> statsNode, String indentContent,
			TimeUnit timeUnit, int id, StatsMarker... markers) {
		ArrayList<Tuple8<StatsNode<?>, String, String, String, String, Long, String, String>> chainOrder = new ArrayList<>();
		StringBuilder sb = new StringBuilder();

		while (statsNode != null) {
			String operator = statsNode.operatorName();
			String line = statsNode.line();
			String requested = requested(statsNode);
			String produced = produced(statsNode);
			long avgProcessingTime = avgProcessingTime(statsNode);
			String lastSignal = lastObservedSignal(statsNode);
			String state = state(statsNode);
			chainOrder.add(0, Tuples.of(statsNode, operator, line, requested, produced, avgProcessingTime, lastSignal, state));
			statsNode = statsNode.upstreamNode;
		}

		int maxOperatorWidth = 0;
		int maxRequestWidth = 0;
		int maxProduceWidth = 0;
		int maxAvgProcessingTimeWidth = 0;
		int maxLastSignalWidth = 0;
		int maxStateWidth = 0;
		for (Tuple8<StatsNode<?>, String, String, String, String, Long, String, String> t : chainOrder) {
			int length = t.getT2().length();
			if (length > maxOperatorWidth) {
				maxOperatorWidth = length;
			}

			length = t.getT4().length();
			if (length > maxRequestWidth) {
				maxRequestWidth = length;
			}

			length = t.getT5().length();
			if (length > maxProduceWidth) {
				maxProduceWidth = length;
			}

			Long value = t.getT6();
			length = String.valueOf(value).length();
			if (length > maxAvgProcessingTimeWidth) {
				maxAvgProcessingTimeWidth = length;
			}

			length = t.getT7().length();
			if (length > maxLastSignalWidth) {
				maxLastSignalWidth = length;
			}

			length = t.getT8().length();
			if (length > maxStateWidth) {
				maxStateWidth = length;
			}
		}

		maxAvgProcessingTimeWidth -= timeUnit.ordinal() * 3;

		for (int i = 0; i < chainOrder.size(); i++) {
			Tuple8<StatsNode<?>, String, String, String, String, Long, String, String> t = chainOrder.get(i);
			StatsNode<?> statsOperator = t.getT1();
			String operator = t.getT2();
			String message = t.getT3();
			String requested = t.getT4();
			String produced = t.getT5();
			Long avgProcessingTime = t.getT6();
			String lastSignal = t.getT7();
			String state = t.getT8();

			String marker = null;

			for (StatsMarker statsMarker : markers) {
				marker = statsMarker.mark(statsOperator);
				if (marker != null) {
					break;
				}
			}
			sb.append(indentContent);

			if (id < 0 || i > 0) {
				sb.append("| ");
			}
			else {
				String idString = String.valueOf(id);
				sb.replace(indentContent.length() - 1 - idString.length(), indentContent.length() - 1, idString)
				  .append("| ");
			}

			if (marker != null) {
				sb.replace(sb.length() - 5,
						sb.length() - 3, marker);
			}

			int indent = maxOperatorWidth - operator.length();
			StringBuilder indentSpace = new StringBuilder();
			for (int j = 0; j < indent; j++) {
				indentSpace.append(' ');
			}
			sb.append(' ');
			sb.append(operator);
			sb.append(indentSpace);
			sb.append(CALL_SITE_GLUE);
			sb.append(message);
			sb.append("\n");

			boolean hasInners = statsOperator.innerNodes.length > 0;
			if (hasInners) {
				sb.append(indentContent).append("| ");
			}
			else {
				sb.append(indentContent).append("|_");
			}

			for (int j = indent; j < maxOperatorWidth + 1; j++) {
				indentSpace.append(' ');
			}
			sb.append(indentSpace);

			sb.append(PROBE_CALL_SITE_GLUE);
			sb.append("Stats(Requested: ");

			for (int j = 0; j < maxRequestWidth - requested.length(); j++) {
				sb.append(' ');
			}
			sb.append(requested);
			sb.append(';');

			sb.append(" Produced: ");
			for (int j = 0; j < maxProduceWidth - produced.length(); j++) {
				sb.append(' ');
			}
			sb.append(produced);
			sb.append(';');

			sb.append(" OnNext ~Time: ");
			String processingTime = String.valueOf(timeUnit.convert(avgProcessingTime, TimeUnit.NANOSECONDS));
			for (int j = 0; j < maxAvgProcessingTimeWidth - processingTime.length(); j++) {
				sb.append(' ');
			}
			sb.append(processingTime);
			sb.append(TIME_UNIT_SYMBOLS.get(timeUnit));
			sb.append(';');

			sb.append(" Last Signal: ");
			for (int j = 0; j < maxLastSignalWidth - lastSignal.length(); j++) {
				sb.append(' ');
			}
			sb.append(lastSignal);
			sb.append(';');

			sb.append(" State: ");
			for (int j = 0; j < maxStateWidth - state.length(); j++) {
				sb.append(' ');
			}
			sb.append(state);
			sb.append(')');

			if (hasInners) {
				sb.append("\n");

				int length = statsOperator.innerNodes.length;
				for (int j = 0; j < length; j++) {
					StatsNode<?> innerNode = statsOperator.innerNodes[j];
					sb.append(indentContent).append("|\n");
					sb.append(buildReport(innerNode, indentContent + "|  " + indentSpace.toString(), timeUnit, j, markers));
					sb.append("\n");
				}

				sb.append(indentContent).append("|_");
			}

			if (i != chainOrder.size() - 1) {
				sb.append("\n");
			}
		}

		return sb.toString();
	}
}