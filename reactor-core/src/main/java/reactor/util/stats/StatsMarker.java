package reactor.util.stats;

import reactor.util.annotation.Nullable;

@FunctionalInterface
public interface StatsMarker {

	@Nullable
	String mark(StatsNode<?> stats);
}
