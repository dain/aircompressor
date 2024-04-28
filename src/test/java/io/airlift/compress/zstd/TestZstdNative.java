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

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.thirdparty.ZstdJniCompressor;
import io.airlift.compress.thirdparty.ZstdJniDecompressor;
import org.apache.hadoop.io.compress.zstd.ZStandardDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import static java.util.Objects.requireNonNull;

public class TestZstdNative
        extends AbstractTestZstd
{
    static {
        try {
            // loading the zstd library from hadoop causes a segfault
            // without this you only get a test failure (likely also cause by C heap corruption)
            //noinspection ConstantValue
            if (true) {
                loadLibrary("zstd");
            }

            // the hadoop native library is required to load the ZStandardCodec class
            loadLibrary("hadoop");
            Field field = NativeCodeLoader.class.getDeclaredField("nativeCodeLoaded");
            field.setAccessible(true);
            field.set(null, true);

            // Loading the decompressor causes the Hadoop native zstd code to load and that triggers the bug
            if (!ZStandardDecompressor.isNativeCodeLoaded()) {
                throw new ExceptionInInitializerError("ZStandardCodec native code not loaded");
            }
        }
        catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static void loadLibrary(String name)
            throws IOException
    {
        String platform = (System.getProperty("os.name") + "-" + System.getProperty("os.arch")).replace(' ', '_');
        String libraryPath = "/nativelib/" + platform + "/" + System.mapLibraryName(name);
        URL url = TestZstdNative.class.getResource(libraryPath);
        File file = File.createTempFile(name, null);
        try (InputStream in = requireNonNull(url).openStream()) {
            Files.copy(in, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        System.load(file.getAbsolutePath());
    }

    @Override
    protected ZstdCompressor getCompressor()
    {
        return new ZstdNativeCompressor();
    }

    @Override
    protected ZstdDecompressor getDecompressor()
    {
        return new ZstdNativeDecompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new ZstdJniCompressor(3);
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new ZstdJniDecompressor();
    }
}
