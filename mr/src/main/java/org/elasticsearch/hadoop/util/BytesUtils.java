/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.util;

import java.util.Arrays;

public abstract class BytesUtils {

    /**
     * Counts the chars within a given, UTF-8 stream and matches the given char positions to the stream
     * byte positions, which are being returned.
     *
     * @param stream UTF-8 byte stream
     * @param charPositions char positions to be matched in the stream
     * @return byte positions matching the char ones
     */
    // the algo is pretty simple:
    // 1. sorts out the char positions for easy matching
    // 2. iterate through the stream and count each char position
    // 3. match the byte positions back to the original char positions
	public static int[] charToBytePosition(BytesArray ba, int... charPositions) {

		int[] results = Arrays.copyOf(charPositions, charPositions.length);
        Arrays.fill(results, -1);
		int[] inputCopy = Arrays.copyOf(charPositions, charPositions.length);
        // sort positions (just in case)
        Arrays.sort(inputCopy);

        int charPosIndex = 0;
        int currentCharPos = 0;

		int byteIndex = ba.offset;
        final int limit = ba.size;
		byte[] bytes = ba.bytes;

		while (byteIndex < limit) {
			int b = bytes[byteIndex] & 0xff;
			int delta = (b < 0xc0 ? 1 : b < 0xe0 ? 2 : b < 0xf0 ? 3 : 4);
			while (inputCopy[charPosIndex] == currentCharPos) {
				results[charPosIndex] = byteIndex;
				if (charPosIndex + 1 < inputCopy.length) {
					charPosIndex++;
				}
				else {
					break;
                 }
             }
			byteIndex += delta;
			currentCharPos++;
		}

		// return the results according to the original char position
		// as there might be duplicates (which mess out sorting) do a copy
		int[] finalResults = Arrays.copyOf(results, results.length);
		for (int originalPosition = 0; originalPosition < charPositions.length; originalPosition++) {
			int sortedPosition = Arrays.binarySearch(inputCopy, charPositions[originalPosition]);
			finalResults[originalPosition] = results[sortedPosition];
		}

		return finalResults;
	}

	/**
	 * Removes the white space from the given byte array. White space is defined in the context of UTF-8 JSON
	 * aka space, horizontal tab, line feed and carriage return.
	 *
	 * @param source
	 * @param offset
	 * @param length
	 * @return
	 */
	public static int trimLeft(byte[] source, int start, int stop) {
		for (int i = start; i < stop; i++) {
			if (!isWhitespace(source[i])) {
				return i;
			}
		}
		return stop;
	}


	public static int trimRight(byte[] source, int start, int stop) {
		for (int i = stop; i > start; i--) {
			if (!isWhitespace(source[i])) {
				return i;
            }
        }
		return start;
    }

	private static boolean isWhitespace(byte current) {
		return current == 0x20 || current == 0x0d || current == 0x0a || current == 0x09;
    }
}