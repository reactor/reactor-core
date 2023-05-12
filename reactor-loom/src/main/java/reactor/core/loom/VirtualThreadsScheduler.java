package reactor.core.loom;

import java.util.concurrent.Executors;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class VirtualThreadsScheduler {

	static Scheduler instance() {
		return Schedulers.fromExecutorService(Executors.newVirtualThreadPerTaskExecutor());
	}
}
