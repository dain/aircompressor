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
package io.airlift.compress.zstd;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Objects;

import static io.airlift.compress.zstd.ZstdNative.DEFAULT_COMPRESSION_LEVEL;
import static java.lang.Math.toIntExact;

public class ZstdNativeCompressor
        implements ZstdCompressor
{
    private final int compressionLevel;

    public ZstdNativeCompressor()
    {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    public ZstdNativeCompressor(int compressionLevel)
    {
        this.compressionLevel = compressionLevel;
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return toIntExact(ZstdNative.maxCompressedLength(uncompressedSize));
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        Objects.checkFromIndexSize(inputOffset, inputLength, input.length);
        Objects.checkFromIndexSize(outputOffset, maxOutputLength, output.length);
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return toIntExact(compress(inputSegment, outputSegment));
    }

    @Override
    public void compress(ByteBuffer inputBuffer, ByteBuffer outputBuffer)
    {
        MemorySegment inputSegment = MemorySegment.ofBuffer(inputBuffer);
        MemorySegment outputSegment = MemorySegment.ofBuffer(outputBuffer);
        int compressedSize = compress(inputSegment, outputSegment);
        outputBuffer.position(outputBuffer.position() + compressedSize);
    }

    @Override
    public int compress(MemorySegment inputSegment, MemorySegment outputSegment)
    {
        return toIntExact(ZstdNative.compress(inputSegment, outputSegment, compressionLevel));
    }
}
