package reactor.core.subscriber;

/**
 * A lifecycle backed downstream
 */
public interface SubscriberState {

	/**
	 * @return expected number of events to be produced to this component
	 */
	default long expectedFromUpstream() {
		return -1L;
	}

	/**
	 * Return defined element capacity
	 * @return long capacity
	 */
	default long getCapacity() {
		return -1L;
	}


	/**
	 * Current error if any, default to null
	 * @return Current error if any, default to null
	 */
	default Throwable getError(){
		return null;
	}

	/**
	 * Return current used space in buffer
	 * @return long capacity
	 */
	default long getPending() {
		return -1L;
	}

	/**
	 *
	 * @return has the downstream "cancelled" and interrupted its consuming ?
	 */
	default boolean isCancelled() { return false; }

	/**
	 * Has this upstream started or "onSubscribed" ?
	 * @return has this upstream started or "onSubscribed" ?
	 */
	default boolean isStarted() {
		return false;
	}

	/**
	 * Has this upstream finished or "completed" / "failed" ?
	 * @return has this upstream finished or "completed" / "failed" ?
	 */
	default boolean isTerminated() {
		return false;
	}

	/**
	 * @return a given limit threshold to replenish outstanding upstream request
	 */
	default long limit() {
		return -1L;
	}

	/**
	 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription} request needs.
	 * This is the maximum in-flight data allowed to transit to this elements.
	 * @return long capacity
	 */
	default long requestedFromDownstream(){
		return -1L;
	}
}
