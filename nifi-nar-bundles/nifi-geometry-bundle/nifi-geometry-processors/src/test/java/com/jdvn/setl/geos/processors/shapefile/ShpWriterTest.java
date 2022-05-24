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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import org.apache.nifi.flowfile.attributes.GeoAttributes;
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
        
        final List<Field> crimialTraceFields = new ArrayList<>();
        crimialTraceFields.add(new Field("the_geom", Schema.create(Type.STRING), null, (Object) null));
        crimialTraceFields.add(new Field("NAME", Schema.create(Type.STRING), null, (Object) null));
        crimialTraceFields.add(new Field("PRIORITY", Schema.create(Type.INT), null, (Object) null));
        final Schema schema = Schema.createRecord("CrimialTrace", null, null, false);
        schema.setFields(crimialTraceFields);
        
        final byte[] source;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            final DataFileWriter<GenericRecord> writer = dataFileWriter.create(schema, baos)) {


            final GenericRecord drugRecord = new GenericData.Record(schema);
            drugRecord.put("the_geom", "MULTILINESTRING ((964395.3586605055 1940987.5770530214, 964375.6926994757 1940991.1736330346))");
            drugRecord.put("NAME", "Drug");
            drugRecord.put("PRIORITY", 1);
            writer.append(drugRecord);
            
            final GenericRecord StoleRecord = new GenericData.Record(schema);
            StoleRecord.put("the_geom", "MULTILINESTRING ((961078.5650489785 1945008.1734879282, 961072.9743108985 1945022.0178549928))");
            StoleRecord.put("NAME", "Stole");
            StoleRecord.put("PRIORITY", 2);
            writer.append(StoleRecord);
        }        
        source = baos.toByteArray();
        String wkt = "PROJCS[\"VN_2000_UTM_Zone_48N\", \r\n" + 
        		"  GEOGCS[\"GCS_VN-2000\", \r\n" + 
        		"    DATUM[\"D_Vietnam_2000\", \r\n" + 
        		"      SPHEROID[\"WGS_1984\", 6378137.0, 298.257223563], \r\n" + 
        		"      TOWGS84[-192.873, -39.382, -111.202, -0.00205, 0.0005, -0.00335, 0.0188], \r\n" + 
        		"      AUTHORITY[\"EPSG\",\"6756\"]], \r\n" + 
        		"    PRIMEM[\"Greenwich\", 0.0], \r\n" + 
        		"    UNIT[\"degree\", 0.017453292519943295], \r\n" + 
        		"    AXIS[\"Longitude\", EAST], \r\n" + 
        		"    AXIS[\"Latitude\", NORTH], \r\n" + 
        		"    AUTHORITY[\"EPSG\",\"4756\"]], \r\n" + 
        		"  PROJECTION[\"Transverse_Mercator\"], \r\n" + 
        		"  PARAMETER[\"central_meridian\", 107.75], \r\n" + 
        		"  PARAMETER[\"latitude_of_origin\", 0.0], \r\n" + 
        		"  PARAMETER[\"scale_factor\", 0.9999], \r\n" + 
        		"  PARAMETER[\"false_easting\", 500000.0], \r\n" + 
        		"  PARAMETER[\"false_northing\", 0.0], \r\n" + 
        		"  UNIT[\"m\", 1.0], \r\n" + 
        		"  AXIS[\"x\", EAST], \r\n" + 
        		"  AXIS[\"y\", NORTH], \r\n" + 
        		"  AUTHORITY[\"VN\",\"10775\"]]";
        Map<String, String> attributes = new HashMap<>();
        attributes.put(GeoAttributes.CRS.key(), wkt);
        attributes.put(CoreAttributes.FILENAME.key(), "sample.shp");
        runner.enqueue(source, attributes);
        runner.run();
        Path targetPath = Paths.get(TARGET_DIRECTORY + "/new-folder/sample.shp");
        byte[] content = Files.readAllBytes(targetPath);
        assertEquals(source, content);
    }

}
