/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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


package reactor.io.buffer

import reactor.core.support.ReactiveState
import spock.lang.Specification

import java.nio.BufferOverflowException
import java.nio.ByteBuffer

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class BufferSpec extends Specification {

	def "A Buffer can be created from a String"() {
		when: "a Buffer is created from a String"
			def buff = Buffer.wrap("Hello World!")

		then: "the Buffer contains the String"
			buff.asString() == "Hello World!"
	}

	def "A Buffer can be appended"() {
		when: "a Buffer is created from a String"
			def buff = Buffer.wrap("Hello World!")
			buff.append(new Buffer())

		then: "the Buffer contains the String"
			buff.asString() == "Hello World!"
	}


	def "A Buffer accepts special characters"() {
		when: "a Buffer is created from a special char"
			def buff = Buffer.wrap("\u2026")

		then: "the Buffer contains the String"
			buff.asString() == "\u2026"
	}

	def "A fixed-length Buffer can be created from a String"() {
		given: "a fixed-length Buffer is created"
			def buff = Buffer.wrap("Hello", true)

		when: "an attempt is made to append data to the Buffer"
			buff.append(" World!")

		then: "an error is thrown"
			thrown(BufferOverflowException)
	}

	def "A Buffer prepends a Buffer to an existing Buffer"() {
		given: "an full Buffer"
			def buff = Buffer.wrap("World!", false)

		when: "another Buffer is prepended"
			buff.prepend(Buffer.wrap("Hello "))

		then: "the Buffer was prepended"
			buff.asString() == "Hello World!"
	}

	def "A Buffer prepends Strings to an existing Buffer"() {
		given: "an full Buffer"
			def buff = Buffer.wrap("World!", false)

		when: "a String is prepended"
			buff.prepend("Hello ")

		then: "the String was prepended"
			buff.asString() == "Hello World!"
	}

	def "A Buffer prepends primitives to an existing Buffer"() {
		given: "a Buffer with data"
			def buff = Buffer.wrap("5", false)

		when: "a byte is prepended"
			buff.prepend((byte) 52)

		then: "the byte was prepended"
			buff.asString() == "45"

		when: "a char is prepended"
			buff.prepend((char) 51)

		then: "the char was prepended"
			buff.readChar() == '3'

		when: "an int is prepended"
			buff.prepend(2)

		then: "the int was prepended"
			buff.readInt() == 2

		when: "a long is prepended"
			buff.prepend(1L)

		then: "the long was prepended"
			buff.readLong() == 1L
	}

	def "A Buffer reads and writes Buffers"() {
		given: "an empty Buffer and a full Buffer"
			def buff = new Buffer()
			def fullBuff = Buffer.wrap("Hello World!")

		when: "a Buffer is appended"
			buff.append(fullBuff)

		then: "the Buffer was added"
			buff.position() == 12
			buff.flip().asString() == "Hello World!"
	}

	def "A Buffer reads and writes Strings"() {
		given: "an empty Buffer"
			def buff = new Buffer()

		when: "a String is appended"
			buff.append("Hello World!")

		then: "the String was added"
			buff.position() == 12
			buff.flip().asString() == "Hello World!"
	}

	def "A Buffer reads and writes primitives"() {
		given: "an empty Buffer"
			def buff = new Buffer()

		when: "a byte is appended"
			buff.append((byte) 1)

		then: "the byte was added"
			buff.position() == 1
			buff.flip().read() == 1

		when: "a char is appended"
			buff.clear()
			buff.append((char) 1)

		then: "the char was added"
			buff.position() == 2
			buff.flip().readChar() == (char) 1

		when: "an int is appended"
			buff.clear()
			buff.append(1)

		then: "the int was added"
			buff.position() == 4
			buff.flip().readInt() == 1

		when: "a long is appended"
			buff.clear()
			buff.append(1L)

		then: "the long was added"
			buff.position() == 8
			buff.flip().readLong() == 1L
	}

	def "A Buffer provides position, limit, capacity, and remaining"() {
		given: "a full Buffer"
			def buff = Buffer.wrap("Hello World!")

		when: "take is checked"
			def limit = buff.limit()

		then: "a take is provided"
			limit == 12

		when: "capacity is checked"
			def cap = buff.capacity()

		then: "a capacity is provided"
			cap == 12

		when: "position is checked"
			def pos = buff.position()

		then: "a position is provided"
			pos == 0
	}

	def "A Buffer can have first and last positions read"() {
		given: "a full Buffer"
			def buff = Buffer.wrap("Hello World!")

		when: "the first byte is checked"
			def first = buff.first()

		then: "the first byte is an H"
			first == (byte) 72

		when: "the last byte is checked"
			def last = buff.last()

		then: "the last byte is an !"
			last == (byte) 33
	}

	def "A Buffer provides an iterator over each byte"() {
		given: "a full Buffer"
			def buff = Buffer.wrap("Hello World!")
			def count = 0

		when: "the bytes are iterated over"
			buff.each { b ->
				count++
			}

		then: "the count should be 12"
			count == 12
	}

	def "A Buffer can be efficiently substringed"() {
		given: "a full Buffer"
			def buff = Buffer.wrap("Hello World!")

		when: "a substring is extracted"
			def substr = buff.substring(6, 11)

		then: "the substring was extracted"
			substr == "World"
	}

	def "A Buffer is also a ReadableByteChannel and WritableByteChannel"() {
		given: "an empty Buffer as a WritableByteChannel"
			def buff = new Buffer(12, true)

		when: "a ByteBuffer is written into the Buffer"
			def bb = ByteBuffer.wrap("Hello World!".bytes)
			buff.write(bb)

		then: "the Buffer had data written to it"
			buff.flip().asString() == "Hello World!"

		when: "a ByteBuffer is read from the Buffer"
			bb = ByteBuffer.allocate(5)
			buff.read(bb)

		then: "the ByteBuffer has data in it"
			bb.position() == 5
			buff.position() == 5
			bb.flip().get() == (byte) 72
	}

	def "A Buffer can be split into segments based on a delimiter"() {
		given: "a full Buffer"
			def buff = Buffer.wrap("Hello World!\nHello World!\nHello World!")

		when: "the Buffer is split"
			def parts = buff.split(10)

		then: "there are only 2 parts"
			parts.size() == 2
	}

	def "Splitting a single-segment buffer yields a single part with the expected contents"() {
		given: "A buffer with a single segment"
			def buff = Buffer.wrap("Hello World!\n")

		when: "the buffer is split"
			def parts = buff.split((int) '\n')

		then: "there is a single part"
			parts.size() == 1
			def strings = []
			parts.each { part -> strings << new String(part.get().asBytes()) }
			strings == ['Hello World!\n']
	}

	def "Splitting a two-segment buffer yields two parts with the expected contents"() {
		given: "A buffer with two segments"
			def buff = Buffer.wrap("Hello World!\nHello World!\n")

		when: "the buffer is split"
			def parts = buff.split((int) '\n')

		then: "there are two parts"
			parts.size() == 2
			def strings = []
			parts.each { part -> strings << new String(part.get().asBytes()) }
			strings == ['Hello World!\n', 'Hello World!\n']
	}

	def "A buffer can be split on a delimiter and the delimiter can be stripped from each segment"() {
		given: "A buffer with three segments"
			def buff = Buffer.wrap("One\nTwo\nThree\n")

		when: "the buffer is split on the delimiter and the delimiter is stripped"
			def parts = buff.split(10, true)

		then: "three parts with the expected contents are produced"
			def strings = []
			parts.each { part -> strings << part.get().asString() }
			strings.size() == 3
			strings == ['One', 'Two', 'Three']
	}

	def "A buffer can be split on a delimiter of multiple byte length"() {
		given:
			"A buffer with three segments"
			def buff = Buffer.wrap "One\r\nTwo\r\nThree\r\n"
			def delim = Buffer.wrap "\r\n"

		when:
			"the buffer is split on the delimiter and the delimiter is stripped"
			def parts = buff.split(delim, true)

		then:
			"three parts with the expected contents are produced"
			def strings = []
			parts.each { part -> strings << part.get().asString() }
			strings.size() == 3
			strings == ['One', 'Two', 'Three']
	}

	def "A Buffer can be sliced into segments"() {
		given: "a syslog message, buffered"
			def buff = Buffer.wrap("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8\n")

		when: "positions are assigned and the buffer is sliced"
			def positions = [1, 3, 4, 19, 20, 29, 30] as int[]
			def slices = buff.slice(positions)

		then: "the buffer is sliced"
			slices[0].get().asString() == "34"
			slices[1].get().asString() == "Oct 11 22:14:15"
			slices[2].get().asString() == "mymachine"
			slices[3].get().asString() == "su: 'su root' failed for lonvick on /dev/pts/8\n"
	}

	def "A Buffer rejects an attempt to rewind by a negative number of bytes"() {
		given: "A buffer"
			def buffer = Buffer.wrap("some data")

		when: "The buffer is rewound by a negative number of bytes"
			buffer.rewind(-5)

		then: "An IllegalArgumentException is thrown"
			thrown(IllegalArgumentException)
	}

	def "A Buffer rejects an attempt to skip a negative number of bytes"() {
		given: "A buffer"
			def buffer = Buffer.wrap("some data")

		when: "The buffer is asked to skip a negative number of bytes"
			buffer.skip(-5)

		then: "An IllegalArgumentException is thrown"
			thrown(IllegalArgumentException)
	}

	def "An IllegalArgumentException is thrown if a buffer is asked to skip beyond its end"() {
		given: "A buffer"
			def buffer = Buffer.wrap("some data")

		when: "The buffer is asked to skip beyond its end"
			buffer.skip(100)

		then: "An IllegalArgumentException is thrown"
			thrown(IllegalArgumentException)
	}

	def "An IllegalArgumentException is thrown if a buffer is asked to rewind beyond its beginning"() {
		given: "A buffer"
			def buffer = Buffer.wrap("some data")

		when: "The buffer is asked to rewind beyond its beginning"
			buffer.rewind(100)

		then: "An IllegalArgumentException is thrown"
			thrown(IllegalArgumentException)
	}

	def "A Buffer can be duplicated"() {
		given: "A Buffer"
			def buffer = new Buffer(100, true).append("Hello World!").flip()

		when: "the Buffer is duplicated"
			def dup = buffer.duplicate()

		then: "a new Buffer is created on a duplicate"
			dup.capacity() == 100
			dup.asString() == "Hello World!"
	}

	def "A Buffer can be copied"() {
		given: "A Buffer"
			def buffer = new Buffer(100, true).append("Hello World!").flip()

		when: "the Buffer is copied"
			def copy = buffer.copy()

		then: "a new Buffer is created on a copy"
			copy.capacity() == ReactiveState.SMALL_IO_BUFFER_SIZE
			copy.asString() == "Hello World!"
	}

	def "A Buffer can be searched"() {
		given: "A Buffer"
			def buffer = Buffer.wrap("Hello World!")

		when: "the Buffer is searched"
			def pos = buffer.indexOf((byte) 0x21)

		then: "the char is found"
			pos == 12

		when: "the Buffer is searched within a range"
			pos = buffer.indexOf((byte) 0x21, 0, 11)

		then: "the char is not found"
			pos == -1
	}

	def "A dynamic Buffer with no initial size can be expanded"() {
		given: "A dynamic Buffer without param"
			def buffer = new Buffer()
			def originalCapacity = buffer.capacity()

		when: "the Buffer is appended with 30 bytes"
			int dataSize = 30
			buffer.append(new byte[dataSize])

		then: "the capacity has not changed"
			buffer.capacity() == originalCapacity
			buffer.position() == dataSize
	}

	def "A dynamic Buffer can be expanded"() {
		given: "A dynamic Buffer starting with 32 kb"
			def originalCapacity = 32 * 1024
			def buffer = new Buffer(originalCapacity, false)

		expect: "the capacity is equals to originalCapacity"
			buffer.capacity() == originalCapacity

		when: "the Buffer is appended with 4 kb"
			int dataSize = 4 * 1024
			buffer.append(new byte[dataSize])

		then: "the capacity has not changed"
			buffer.capacity() == originalCapacity
			buffer.position() == dataSize

		when: "the Buffer is appended with 64 kb"
			dataSize = 64 * 1024
			buffer.append(new byte[dataSize])

		then: "the capacity should now be higher than original capacity"
			buffer.capacity() >= originalCapacity
	}

	def "A fixed Buffer cannot be expanded"() {
		given: "A fixed Buffer with 8 kb"
			def buffer = new Buffer(8 * 1024, true)

		when: "the Buffer is appended with 24 kb"
			int dataSize = 24 * 1024
			buffer.append(new byte[dataSize])

		then: "a BufferOverflowException is thrown"
			thrown(BufferOverflowException)
	}
}
