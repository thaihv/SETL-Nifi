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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
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
    public void testShapeFilesListPickedUp() throws IOException {
        final File directory = new File("src/test/resources/koreanmap");
        final File inFile = new File("src/test/resources/koreanmap/LV14_SPBD_BULD.shp");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        final Path absTargetPath = targetPath.toAbsolutePath();
        final String absTargetPathStr = absTargetPath.getParent() + "/";
        Files.copy(inPath, targetPath);

        final TestRunner runner = TestRunners.newTestRunner(new ShpReader());
        runner.setProperty(ShpReader.DIRECTORY, directory.getAbsolutePath());
        runner.setProperty(ShpReader.FILE_FILTER, ".*\\.shp");
        //runner.setProperty(ShpReader.FILE_FILTER, "LV14_SPBD_BULD.shp");
        
        runner.run();

        runner.assertAllFlowFilesTransferred(ShpReader.REL_SUCCESS, 3);  // Batch Size = 10 default
        runner.assertTransferCount(ShpReader.REL_SUCCESS, 3);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ShpReader.REL_SUCCESS);

        final String path = successFiles.get(0).getAttribute("path");
        assertEquals("/", path);
        final String absolutePath = successFiles.get(0).getAttribute(CoreAttributes.ABSOLUTE_PATH.key());
        assertEquals(absTargetPathStr, absolutePath);    	
    	

    }
    @Test
    public void testAShapeFilePickedUp() throws IOException {
        final File directory = new File("src/test/resources/koreanmap");

        final TestRunner runner = TestRunners.newTestRunner(new ShpReader());
        runner.setProperty(ShpReader.DIRECTORY, directory.getAbsolutePath());
        runner.setProperty(ShpReader.FILE_FILTER, "LV14_SPBD_BULD.shp");
        runner.run();

        runner.assertAllFlowFilesTransferred(ShpReader.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ShpReader.REL_SUCCESS);

        final String path = successFiles.get(0).getAttribute("path");
        assertEquals("/", path);
        final String absolutePath = successFiles.get(0).getAttribute(CoreAttributes.FILENAME.key());
        System.out.println(absolutePath);
        final String mimeType = successFiles.get(0).getAttribute(CoreAttributes.MIME_TYPE.key());
        System.out.println(mimeType);
    }
    @Test
    public void testAttributes() throws IOException {
        final File directory = new File("src/test/resources/admzone");
        final File inFile = new File("src/test/resources/admzone/ADMZONE.shp");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        Files.copy(inPath, targetPath);

        boolean verifyLastModified = false;
        try {
            destFile.setLastModified(1000000000);
            verifyLastModified = true;
        } catch (Exception doNothing) {
        }

        final TestRunner runner = TestRunners.newTestRunner(new ShpReader());
        runner.setProperty(ShpReader.DIRECTORY, directory.getAbsolutePath());
        runner.setProperty(ShpReader.FILE_FILTER, "ADMZONE.shp");
        runner.run();

        runner.assertAllFlowFilesTransferred(ShpReader.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ShpReader.REL_SUCCESS);

        if (verifyLastModified) {
            try {
                final DateFormat formatter = new SimpleDateFormat(ShpReader.FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                final Date fileModifyTime = formatter.parse(successFiles.get(0).getAttribute("file.lastModifiedTime"));
                assertEquals(new Date(1000000000), fileModifyTime);
            } catch (ParseException e) {
                fail();
            }
        }
    }
    @Test
    public void testDefaultProperties() throws IOException {
        final File directory = new File("C:\\Download\\setl_in");

        final TestRunner runner = TestRunners.newTestRunner(new ShpReader());
        runner.setProperty(ShpReader.DIRECTORY, directory.getAbsolutePath());
        runner.setProperty(ShpReader.FILE_FILTER, "HaNoi_communes.shp");
        runner.run();

        runner.assertAllFlowFilesTransferred(ShpReader.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ShpReader.REL_SUCCESS);

        final String path = successFiles.get(0).getAttribute("path");
        assertEquals("/", path);
        final String absolutePath = successFiles.get(0).getAttribute(CoreAttributes.FILENAME.key());
        System.out.println(absolutePath);
        final String mimeType = successFiles.get(0).getAttribute(CoreAttributes.MIME_TYPE.key());
        System.out.println(mimeType);

    }    
}
