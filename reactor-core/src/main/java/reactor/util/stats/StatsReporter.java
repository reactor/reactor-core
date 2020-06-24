package reactor.util.stats;

public interface StatsReporter {

	void report(StatsNode<?> statsNode, StatsMarker... markers);
}