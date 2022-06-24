/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jdvn.setl.geos.processors.geopackage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class GeoPackageWriterTest {

    public static final String TARGET_DIRECTORY = "target/put-geopackge";
    private File targetDir;

    @Before
    public void prepDestDirectory() throws IOException {
        targetDir = new File(TARGET_DIRECTORY);
        if (!targetDir.exists()) {
            Files.createDirectories(targetDir.toPath());
            return;
        }

        targetDir.setReadable(true);

        deleteDirectoryContent(targetDir);
    }

    private void deleteDirectoryContent(File directory) throws IOException {
        for (final File file : directory.listFiles()) {
            if (file.isDirectory()) {
                deleteDirectoryContent(file);
            }
            Files.delete(file.toPath());
        }
    }
    
    public static String humanReadableByteCountBin(long bytes) {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) {
            return bytes + " B";
        }
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum(bytes);
        return String.format("%.1f %ciB", value / 1024.0, ci.current());
    }    
    @Test
    public void testGeopackgeFromFlowfile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new GeoPackageWriter());
        runner.setProperty(GeoPackageWriter.GEOPACKAGE_FILE_NAME, targetDir.getAbsolutePath() + "/mytest.gpkg");
        runner.run();
        Path location = targetDir.toPath();
        System.out.println("Location: " + location.getFileName());
        System.out.println(humanReadableByteCountBin(location.toFile().getTotalSpace()));
        System.out.println(humanReadableByteCountBin(location.toFile().getUsableSpace()));
        File f = new File(targetDir.getAbsolutePath() + "/mytest.gpkg");
        System.out.println(f.getName() + " --> Total Size :" + humanReadableByteCountBin(f.getTotalSpace()) + " ---> Useable Size :" + humanReadableByteCountBin(f.getUsableSpace()));
        
    }
}
