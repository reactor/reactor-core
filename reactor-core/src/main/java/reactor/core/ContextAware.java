package reactor.core;

import reactor.util.context.Context;

public interface ContextAware {
	Context currentContext();
}
