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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;



public class ShpWriterTest {

    public static final String TARGET_DIRECTORY = "target/put-shpfile";
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

    @Test
    public void testCreateDirectory() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ShpWriter());
        String newDir = targetDir.getAbsolutePath()+"/new-folder";
        runner.setProperty(ShpWriter.DIRECTORY, newDir);
        runner.setProperty(ShpWriter.CONFLICT_RESOLUTION, ShpWriter.REPLACE_RESOLUTION);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Hello world!!".getBytes(), attributes);
        runner.run();
        Path targetPath = Paths.get(TARGET_DIRECTORY + "/new-folder/targetFile.txt");
        byte[] content = Files.readAllBytes(targetPath);
        assertEquals("Hello world!!", new String(content));
    }
    
    @Test
    public void testShpFileFromFlowfile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ShpWriter());
        String newDir = targetDir.getAbsolutePath()+"/new-folder";
        runner.setProperty(ShpWriter.DIRECTORY, newDir);
        runner.setProperty(ShpWriter.CONFLICT_RESOLUTION, ShpWriter.REPLACE_RESOLUTION);
        
        final List<Field> dogFields = new ArrayList<>();
        dogFields.add(new Field("dogTailLength", Schema.create(Type.INT), null, (Object) null));
        dogFields.add(new Field("dogName", Schema.create(Type.STRING), null, (Object) null));
        final Schema schema = Schema.createRecord("dog", null, null, false);
        schema.setFields(dogFields);
        
        final byte[] source;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            final DataFileWriter<GenericRecord> writer = dataFileWriter.create(schema, baos)) {


            final GenericRecord dogRecord = new GenericData.Record(schema);
            dogRecord.put("dogTailLength", 14);
            dogRecord.put("dogName", "Fido");


            writer.append(dogRecord);
        }        
        source = baos.toByteArray();
        
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "sample.shp");
        runner.enqueue(source, attributes);
        runner.run();
        Path targetPath = Paths.get(TARGET_DIRECTORY + "/new-folder/sample.shp");
        byte[] content = Files.readAllBytes(targetPath);
        assertEquals(source, content);
    }

}
