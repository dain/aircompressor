/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.v3.lz4;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static io.airlift.compress.v3.lz4.Lz4Native.DEFAULT_ACCELERATION;
import static java.lang.Math.toIntExact;

public final class Lz4NativeCompressor
        implements Lz4Compressor
{
    private final MemorySegment state = Arena.ofAuto().allocate(Lz4Native.STATE_SIZE);

    public Lz4NativeCompressor()
    {
        Lz4Native.verifyEnabled();
    }

    public static boolean isEnabled()
    {
        return Lz4Native.isEnabled();
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return Lz4Native.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return Lz4Native.compress(inputSegment, inputLength, outputSegment, maxOutputLength, DEFAULT_ACCELERATION, state);
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        return Lz4Native.compress(input, toIntExact(input.byteSize()), output, toIntExact(output.byteSize()), DEFAULT_ACCELERATION, state);
    }
}