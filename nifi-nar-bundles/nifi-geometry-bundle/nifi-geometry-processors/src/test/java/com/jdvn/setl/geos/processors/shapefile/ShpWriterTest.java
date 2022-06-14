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

import static org.junit.Assert.assertNotNull;

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
    public void testShpFilePolygonFromFlowfile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ShpWriter());
        runner.setProperty(ShpWriter.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(ShpWriter.CONFLICT_RESOLUTION, ShpWriter.REPLACE_RESOLUTION);
        
        final List<Field> buildingFields = new ArrayList<>();
        buildingFields.add(new Field("the_geom", Schema.create(Type.STRING), null, (Object) null));
        buildingFields.add(new Field("ROAD_NM", Schema.create(Type.STRING), null, (Object) null));
        buildingFields.add(new Field("BUL_MAN_NO", Schema.create(Type.LONG), null, (Object) null));
        buildingFields.add(new Field("BDTYP_CD", Schema.create(Type.STRING), null, (Object) null));
        final Schema schema = Schema.createRecord("Buildings", null, null, false);
        schema.setFields(buildingFields);
        
        final byte[] source;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            final DataFileWriter<GenericRecord> writer = dataFileWriter.create(schema, baos)) {


            final GenericRecord b1Record = new GenericData.Record(schema);
            b1Record.put("the_geom", "MULTIPOLYGON (((957993.9864196742 1945576.9570123316, 957983.4609443424 1945573.108225389, 957980.7670381214 1945580.2958235308, 957991.3514169772 1945584.31923318, 957993.9864196742 1945576.9570123316)))");
            b1Record.put("ROAD_NM", "Hakdong-ro 4-gil");
            b1Record.put("BUL_MAN_NO", 21345L);
            b1Record.put("BDTYP_CD", "01001");
            writer.append(b1Record);
            writer.flush();
            
            final GenericRecord b2Record = new GenericData.Record(schema);
            b2Record.put("the_geom", "MULTIPOLYGON (((958025.2624564759 1945601.266386887, 958013.7439423987 1945596.9630322922, 958010.4903963841 1945605.5000932196, 958022.2040601482 1945609.8453978966, 958025.2624564759 1945601.266386887)))");
            b2Record.put("ROAD_NM", "Hakdong-ro 5-gil");
            b2Record.put("BUL_MAN_NO", 21347L);
            b2Record.put("BDTYP_CD", "01011");
            writer.append(b2Record);
            writer.flush();
        }        
        source = baos.toByteArray();
        String wkt = "PROJCS[\"Korea 2000 / Unified CS\", \r\n" + 
        		"  GEOGCS[\"Korea 2000\", \r\n" + 
        		"    DATUM[\"Geocentric datum of Korea\", \r\n" + 
        		"      SPHEROID[\"GRS 1980\", 6378137.0, 298.257222101, AUTHORITY[\"EPSG\",\"7019\"]], \r\n" + 
        		"      TOWGS84[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], \r\n" + 
        		"      AUTHORITY[\"EPSG\",\"6737\"]], \r\n" + 
        		"    PRIMEM[\"Greenwich\", 0.0], \r\n" + 
        		"    UNIT[\"degree\", 0.017453292519943295], \r\n" + 
        		"    AXIS[\"Longitude\", EAST], \r\n" + 
        		"    AXIS[\"Latitude\", NORTH], \r\n" + 
        		"    AUTHORITY[\"EPSG\",\"4737\"]], \r\n" + 
        		"  PROJECTION[\"Transverse_Mercator\"], \r\n" + 
        		"  PARAMETER[\"central_meridian\", 127.5], \r\n" + 
        		"  PARAMETER[\"latitude_of_origin\", 38.0], \r\n" + 
        		"  PARAMETER[\"scale_factor\", 0.9996], \r\n" + 
        		"  PARAMETER[\"false_easting\", 1000000.0], \r\n" + 
        		"  PARAMETER[\"false_northing\", 2000000.0], \r\n" + 
        		"  UNIT[\"m\", 1.0], \r\n" + 
        		"  AXIS[\"x\", EAST], \r\n" + 
        		"  AXIS[\"y\", NORTH], \r\n" + 
        		"  AUTHORITY[\"EPSG\",\"5179\"]]";
        Map<String, String> attributes = new HashMap<>();
        attributes.put(GeoAttributes.CRS.key(), wkt);
        attributes.put(CoreAttributes.FILENAME.key(), "buildings.shp");
        runner.enqueue(source, attributes);
        runner.run();
        
        Path targetPath = Paths.get(TARGET_DIRECTORY + "/buildings.shp");
        byte[] content = Files.readAllBytes(targetPath);
        assertNotNull(content);
    }
    
    @Test
    public void testShpFileMultiPointFromFlowfile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ShpWriter());
        runner.setProperty(ShpWriter.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(ShpWriter.CONFLICT_RESOLUTION, ShpWriter.REPLACE_RESOLUTION);
        
        final List<Field> crimialTraceFields = new ArrayList<>();
        crimialTraceFields.add(new Field("the_geom", Schema.create(Type.STRING), null, (Object) null));
        crimialTraceFields.add(new Field("NAME", Schema.create(Type.STRING), null, (Object) null));
        crimialTraceFields.add(new Field("PRIORITY", Schema.create(Type.INT), null, (Object) null));
        final Schema pointschema = Schema.createRecord("CrimialTrace", null, null, false);
        pointschema.setFields(crimialTraceFields);
        
        final byte[] source;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(pointschema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            final DataFileWriter<GenericRecord> writer = dataFileWriter.create(pointschema, baos)) {


            final GenericRecord drugRecord = new GenericData.Record(pointschema);
            drugRecord.put("the_geom", "MULTIPOINT ((311319.18198472593 2335894.3884669044))");
            drugRecord.put("NAME", "Drug");
            drugRecord.put("PRIORITY", 1);
            writer.append(drugRecord);
            writer.flush();
            
            final GenericRecord StoleRecord = new GenericData.Record(pointschema);
            StoleRecord.put("the_geom", "MULTIPOINT ((311297.08925434074 2335914.745339902))");
            StoleRecord.put("NAME", "Rape");
            StoleRecord.put("PRIORITY", 2);
            writer.append(StoleRecord);
            writer.flush();
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
        attributes.put(CoreAttributes.FILENAME.key(), "point.shp");
        runner.enqueue(source, attributes);
        runner.run();
        Path targetPath = Paths.get(TARGET_DIRECTORY + "/point.shp");
        byte[] content = Files.readAllBytes(targetPath);
        assertNotNull(content);
    }
}
