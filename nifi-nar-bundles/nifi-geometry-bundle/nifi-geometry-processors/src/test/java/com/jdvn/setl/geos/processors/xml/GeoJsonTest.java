
package com.jdvn.setl.geos.processors.xml;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.geotools.data.geojson.GeoJSONReader;
import org.geotools.data.geojson.GeoJSONWriter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class GeoJsonTest {

	@Before
	public void init()  {

	}
	@Test
    public void test_GeoJsonToFeature() throws Exception {
        
		String json = "{\r\n" + 
				"  \"type\": \"Feature\",\r\n" + 
				"  \"geometry\": {\r\n" + 
				"    \"type\": \"Point\",\r\n" + 
				"    \"coordinates\": [125.6, 10.1]\r\n" + 
				"  },\r\n" + 
				"  \"properties\": {\r\n" + 
				"    \"name\": \"Dinagat Islands\"\r\n" + 
				"  }\r\n" + 
				"}";

		
		SimpleFeature sf = GeoJSONReader.parseFeature(json);
		System.out.println(sf);

		try (FileOutputStream output = new FileOutputStream("C:\\Download\\test_geojson.json")) {
			GeoJSONWriter w = new GeoJSONWriter(output);
			w.write(sf);
			w.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

    }
	@Test
    public void test_FeatureToGeoJson() throws Exception {
        File inFile = new File("C:\\Download\\test_geojson.json");
        InputStream targetStream = new FileInputStream(inFile);        
        GeoJSONReader r = new GeoJSONReader(targetStream);
        SimpleFeatureCollection fc = r.getFeatures();
        r.close();
        
        SimpleFeatureIterator iter = fc.features();
        SimpleFeature f = null;
        while( iter.hasNext() ){
            f = iter.next();
            System.out.println(f.getID());
            System.out.println(f.getBounds());
         }
        System.out.println(GeoJSONWriter.toGeoJSON(f));
        
        
        
    }

}
