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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.nifi.avro.AvroReaderWithEmbeddedSchema;
import org.apache.nifi.avro.AvroRecordReader;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.GeoAttributes;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.referencing.CRS;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import com.jdvn.setl.geos.processors.util.GeoUtils;

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
        
        runner.run();

        runner.assertAllFlowFilesTransferred(ShpReader.REL_SUCCESS, 4);  // Batch Size = 10 default
        runner.assertTransferCount(ShpReader.REL_SUCCESS, 4);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ShpReader.REL_SUCCESS);

        final String path = successFiles.get(0).getAttribute("path");
        assertEquals("/", path);
        final String absolutePath = successFiles.get(0).getAttribute(CoreAttributes.ABSOLUTE_PATH.key());
        assertEquals(absTargetPathStr, absolutePath);    	
    	

    }
    @Test
    public void testGeoSpatialDataFlow() throws IOException, FactoryException {
        final File directory = new File("src/test/resources/admzone");
        final TestRunner runner = TestRunners.newTestRunner(new ShpReader());
        runner.setProperty(ShpReader.DIRECTORY, directory.getAbsolutePath());
        runner.setProperty(ShpReader.FILE_FILTER, "CRIMINAL_TRACE.shp");
        
        runner.run();

        runner.assertAllFlowFilesTransferred(ShpReader.REL_SUCCESS, 1);  // Batch Size = 10 default
        runner.assertTransferCount(ShpReader.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ShpReader.REL_SUCCESS);
        final String crs = successFiles.get(0).getAttribute(GeoAttributes.CRS.key());
        final CoordinateReferenceSystem crs_source = CRS.parseWKT(crs);
        assertEquals("VN:VN_2000_UTM_Zone_48N", crs_source.getName().toString());
		AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(successFiles.get(0).getContentStream());
		Record record;
		try {
			while ((record = reader.nextRecord()) != null) {
				String geomFieldName = GeoUtils.getGeometryFieldName(record);
				String geovalue = record.getAsString(geomFieldName);
				assertEquals("MULTIPOINT ((311254.1662353067 2335875.925256511))", geovalue);  // first record
				break;
			}
		} catch (MalformedRecordException e) {
			e.printStackTrace();
		}
        
	
    }    
    @Test
    public void testAttributes() throws IOException {
        final File directory = new File("src/test/resources/admzone");
        final File inFile = new File("src/test/resources/admzone/SPC_DIT_ADMZONE.shp");
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
        runner.setProperty(ShpReader.FILE_FILTER, "SPC_DIT_ADMZONE.shp");
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
        final File directory = new File("src/test/resources/admzone");

        final TestRunner runner = TestRunners.newTestRunner(new ShpReader());
        runner.setProperty(ShpReader.DIRECTORY, directory.getAbsolutePath());
        runner.setProperty(ShpReader.FILE_FILTER, "HaNoi_communes.shp");
        runner.run();

        runner.assertAllFlowFilesTransferred(ShpReader.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ShpReader.REL_SUCCESS);

        final String path = successFiles.get(0).getAttribute("path");
        assertEquals("/", path);
        final String absolutePath = successFiles.get(0).getAttribute(CoreAttributes.FILENAME.key());
        assertEquals("HaNoi_communes.shp", absolutePath);
        final String mimeType = successFiles.get(0).getAttribute(CoreAttributes.MIME_TYPE.key());
        assertEquals("application/avro+geowkt", mimeType);
    }   
    @Test
    public void testProjectedKoreanCoordinatesToDecimalDegree() throws FactoryException, TransformException {
        //Korea EPSG:5179 -> EPSG:4326 CONVERSION
    	
        CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:5179");
        CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:4326");

        double coordinateX = 1307285;
        double coordinateY = 2229260;

        // EPSG:5179 Y , X
        Coordinate in = new Coordinate(coordinateY, coordinateX); 
        Coordinate result = GeoUtils.transformCoordinateBasedOnCrs(sourceCRS,targetCRS,in);
        
        double expectedLongitude = 131.0999928;
        double expectedLatitude = 40.0099721;

        assertEquals(expectedLongitude, result.getY(), 0.00001);
        assertEquals(expectedLatitude, result.getX(), 0.00001);
    } 
    @Test
    public void testProjectedVN2000ToEPSG4326() throws FactoryException, TransformException {
        //VN2000 EPSG:3405 -> EPSG:4326 CONVERSION

        CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:3405");
        CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:4326");

        double coordinateX = 547486.6400756836;
        double coordinateY = 2348240.210083008;
        // EPSG:3405 X , Y
        Coordinate in = new Coordinate(coordinateX, coordinateY); 
        Coordinate result = GeoUtils.transformCoordinateBasedOnCrs(sourceCRS,targetCRS,in);

        double expectedLongitude = 105.4595182204;
        double expectedLatitude =  21.23415875713;

        assertEquals(expectedLongitude, result.getY(), 0.00001);
        assertEquals(expectedLatitude, result.getX(), 0.00001);
    }     
    @Test
    public void testEncodingConversion() throws IOException {
        System.out.println("file.encoding=" + System.getProperty("file.encoding"));
        
        final File file_utf8 = new File("src/test/resources/admzone/TK_OSM_roads_WGS84.shp");
		Map<String, Object> mapAttrs = new HashMap<>();
		mapAttrs.put("url", file_utf8.toURI().toURL());
		DataStore dataStore = DataStoreFinder.getDataStore(mapAttrs);
		String typeName = dataStore.getTypeNames()[0];
		SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeName);
		
		featureSource.getFeatures().size(); // Why need call some functions like this to get right charset ?
		
		Charset charset = ((ShapefileDataStore)dataStore).getCharset(); 
		List<Record> records = GeoUtils.getNifiRecordsFromShapeFile(featureSource, charset);
		String value1 = records.get(0).getAsString("RD_Type");
		String value2 = records.get(0).getAsString("Add_");
		assertEquals("Đường đô thị", value1);
		assertEquals("Hòa Thuận, An Mỹ, An Xuân, An Sơn", value2);     
		
        
        final File file_EUC_KR = new File("src/test/resources/koreanmap/LV14_SPSB_STATN.shp");
        Map<String, Object> mapAttrs_1 = new HashMap<>();
        mapAttrs_1.put("url", file_EUC_KR.toURI().toURL());
        DataStore dataStore_1 = DataStoreFinder.getDataStore(mapAttrs_1);
		String typeName_1 = dataStore_1.getTypeNames()[0];
		SimpleFeatureSource featureSource_1 = dataStore_1.getFeatureSource(typeName_1);
		
		featureSource_1.getFeatures().size();
		Charset charset_1 = ((ShapefileDataStore)dataStore_1).getCharset();
		List<Record> records_1 = GeoUtils.getNifiRecordsFromShapeFile(featureSource_1, charset_1);
		assertEquals("신분당선", records_1.get(0).getAsString("KOR_SBR_NM"));
		assertEquals("강남역", records_1.get(0).getAsString("KOR_SUB_NM")); 

    }   
}
