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
package com.jdvn.setl.geos.processors.shapefile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class ShpReaderTest {

    @Before
    public void init() {
        TestRunners.newTestRunner(ShpReader.class);
    }
    @Test
    public void testFilePickedUp() throws IOException {
        final File directory = new File("target/test/data/in");
        deleteDirectory(directory);
        assertTrue("Unable to create test data directory " + directory.getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/koreanmap/LV14_SPBD_BULD.shp");

        final TestRunner runner = TestRunners.newTestRunner(new ShpReader());
        runner.setProperty(ShpReader.FILENAME, inFile.getAbsolutePath());
        runner.run();

        runner.assertAllFlowFilesTransferred(ShpReader.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ShpReader.REL_SUCCESS);
        successFiles.get(0).assertContentEquals("Hello, World!".getBytes("UTF-8"));

        final String path = successFiles.get(0).getAttribute("path");
        assertEquals("/", path);

    }
    private void deleteDirectory(final File directory) throws IOException {
        if (directory != null && directory.exists()) {
            for (final File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                }

                assertTrue("Could not delete " + file.getAbsolutePath(), file.delete());
            }
        }
    }
    @Test
    public void testProcessor() {

    }

}
