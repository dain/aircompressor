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
package io.airlift.compress.v3.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * A factory for creating Hadoop compliant input and output streams.
 * Implementations of this interface are thread safe.
 */
public interface HadoopStreams
{
    String getDefaultFileExtension();

    List<String> getHadoopCodecName();

    HadoopInputStream createInputStream(InputStream in)
            throws IOException;

    HadoopOutputStream createOutputStream(OutputStream out)
            throws IOException;
}
