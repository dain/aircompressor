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
package io.airlift.compress.snappy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.SymbolLookup;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class NativeLoader
{
    private NativeLoader() {}

    public static SymbolLookup loadLibrary(String name)
    {
        String libraryPath = getLibraryPath(name);
        URL url = NativeLoader.class.getResource(libraryPath);
        if (url == null) {
            throw new LinkageError("Library not found: " + libraryPath);
        }
        Path path = temporaryFile(name, url);
        return SymbolLookup.libraryLookup(path, Arena.ofAuto());
    }

    private static Path temporaryFile(String name, URL url)
    {
        try {
            File file = File.createTempFile(name, null);
            file.deleteOnExit();
            try (InputStream in = url.openStream()) {
                Files.copy(in, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            return file.toPath();
        }
        catch (IOException e) {
            throw new LinkageError("Failed to create temporary file: " + e.getMessage(), e);
        }
    }

    private static String getLibraryPath(String name)
    {
        return "/aircompressor/" + getPlatform() + "/" + System.mapLibraryName(name);
    }

    private static String getPlatform()
    {
        String name = System.getProperty("os.name");
        String arch = System.getProperty("os.arch");
        return (name + "-" + arch).replace(' ', '_');
    }
}
