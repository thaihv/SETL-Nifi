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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.nifi.flowfile.attributes.GeoAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.geopkg.GeoPackage;
import org.geotools.geopkg.GeoPkgDataStoreFactory;
import org.junit.Before;
import org.junit.Test;
import org.opengis.referencing.crs.CoordinateReferenceSystem;


public class GeoPackageReaderTest {

    @Before
    public void init() {
        TestRunners.newTestRunner(GeoPackageReader.class);
    }
    @Test
    public void testGeopackageFilePickedUp() throws IOException {
        final File inFile = new File("src/test/resources/geopackage/utilities.gpkg");

        final TestRunner runner = TestRunners.newTestRunner(new GeoPackageReader());
        runner.setProperty(GeoPackageReader.FILENAME, inFile.getAbsolutePath());
        runner.run();
    }
    @Test
    public void testCRSFromFeatureTable() {
    	
        final String inFile = "src/test/resources/geopackage/hanoi.gpkg";

        final GeoPackageReader toTest = new GeoPackageReader();
        
		HashMap<String, Object> map = new HashMap<>();
		map.put(GeoPkgDataStoreFactory.DBTYPE.key, "geopkg");
		map.put(GeoPkgDataStoreFactory.DATABASE.key, inFile);
		DataStore store = null;
		try {
			store = DataStoreFinder.getDataStore(map);
	        CoordinateReferenceSystem crs = toTest.getCRSFromFeatureTable(store, "communes");
	        assertTrue( crs.getName().toString().contains("EPSG:WGS 84"));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (store != null)
				store.dispose();
		}
		

    }
    @Test
    public void testCRSFromTileTable() {
    	
        final String inFile = "src/test/resources/geopackage/hanoi.gpkg";
        final File geoFile = new File(inFile);
        
        final GeoPackageReader toTest = new GeoPackageReader();
        
        GeoPackage geoPackage;
		try {
			geoPackage = new GeoPackage(geoFile);
			CoordinateReferenceSystem crs = toTest.getCRSFromTilesTable(new File(inFile), geoPackage.tiles().get(0));
			assertTrue( crs.getName().toString().contains("EPSG:WGS 84"));
			geoPackage.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        
		

    }
    @Test
    public void testCheckImageformat() {
    	
        final File inFile = new File("src/test/resources/geopackage/hanoi.gpkg");

        final TestRunner runner = TestRunners.newTestRunner(new GeoPackageReader());
        runner.setProperty(GeoPackageReader.FILENAME, inFile.getAbsolutePath());
        runner.run();
        
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(GeoPackageReader.REL_SUCCESS);
        // flowfile 1 is for tile
        final String geoType1 = successFiles.get(0).getAttribute(GeoAttributes.GEO_TYPE.key());
        final String geoType2 = successFiles.get(1).getAttribute(GeoAttributes.GEO_TYPE.key());
        final String tileFormat = successFiles.get(1).getAttribute(GeoAttributes.GEO_RASTER_TYPE.key());
        assertEquals("Features", geoType1);
        assertEquals("Tiles", geoType2);
        assertEquals("JPEG", tileFormat);

    }    
}
