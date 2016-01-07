package reactor.io.buffer;

import org.junit.Test;
import reactor.core.support.ReactiveState;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Sergey Shcherbakov
 */
public class BufferTests {
/*
	@Test
	@Ignore
	public void testAutoExpand() {

		int initial_small_size = ReactiveState.SMALL_IO_BUFFER_SIZE;
		int initial_max_size = ReactiveState.MAX_IO_BUFFER_SIZE;
		try {
			Buffer b = new Buffer();
			ReactiveState.SMALL_IO_BUFFER_SIZE = 20;        // to speed up the test
			ReactiveState.MAX_IO_BUFFER_SIZE = 100;
			for (int i = 0; i < ReactiveState.MAX_IO_BUFFER_SIZE - ReactiveState.SMALL_IO_BUFFER_SIZE; i++) {
				b.append((byte) 0x1);
			}
		} finally {
			ReactiveState.SMALL_IO_BUFFER_SIZE = initial_small_size;
			ReactiveState.MAX_IO_BUFFER_SIZE = initial_max_size;
		}
	}*/

	@Test
	public void testEquals() {
		Buffer buffer = Buffer.wrap("Hello");

		assertTrue(buffer.equals(Buffer.wrap("Hello")));
		assertFalse(buffer.equals(Buffer.wrap("Other")));
	}

}
