
package com.jdvn.setl.geos.processors.xml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataUtilities;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.WKTReader2;
import org.geotools.wfs.GML;
import org.geotools.wfs.GML.Version;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class GmlTest {

	@Before
	public void init()  {

	}
    /**
     * Check if we can encode a SimpleFeatureType using GML2
     */
    @Test
    public void test_encode_GML2_with_XSD() throws Exception {
        SimpleFeatureType TYPE = DataUtilities.createType("Location", "geom:Point,name:String");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GML encode = new GML(Version.GML2);
        encode.setBaseURL(new URL("http://localhost"));
        encode.encode(out, TYPE);

        out.close();

        String xsd = out.toString();
        System.out.println(xsd);
        assertTrue(xsd.indexOf("gml/2.1.2/feature.xsd") != -1);

    }	
    @Test
    public void test_encodeGML3_with_XSD() throws Exception {
        SimpleFeatureType TYPE = DataUtilities.createType("location", "geom:Point,name:String");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GML encode = new GML(Version.GML3);
        encode.setBaseURL(new URL("http://localhost"));
        encode.setNamespace("location", "http://localhost/location.xsd");
        encode.encode(out, TYPE);

        out.close();

        String xsd = out.toString();
        System.out.println(xsd);
        assertTrue(xsd.indexOf("gml/3.1.1/base/gml.xsd") != -1);
    }
    
	@Test
    public void test_encodeGML2() throws Exception {
        
    	SimpleFeatureType TYPE = DataUtilities.createType("location", "geom:Point,name:String");

    	File locationFile = new File("location.xsd");
    	locationFile = locationFile.getCanonicalFile();
    	
        locationFile.deleteOnExit();
        if (locationFile.exists()) {
            locationFile.delete();
        }
        
    	locationFile.createNewFile();

    	URL locationURL = locationFile.toURI().toURL();
    	URL baseURL = locationFile.getParentFile().toURI().toURL();

    	FileOutputStream xsd = new FileOutputStream(locationFile);

    	GML encode = new GML(Version.GML2);
    	encode.setBaseURL(baseURL);
    	encode.setNamespace("location", locationURL.toExternalForm());
    	encode.encode(xsd, TYPE);

    	xsd.close();

    	WKTReader2 wkt = new WKTReader2();
    	List<SimpleFeature> collection = new LinkedList<SimpleFeature>();
    	collection.add(SimpleFeatureBuilder.build(TYPE, new Object[] { wkt.read("POINT (1 2)"),"name1" }, null));
    	collection.add(SimpleFeatureBuilder.build(TYPE, new Object[] { wkt.read("POINT (4 4)"),"name2" }, null));

    	ByteArrayOutputStream xml = new ByteArrayOutputStream();

    	GML encode2 = new GML(Version.GML2);
    	encode2.setLegacy(true);
    	encode2.setBaseURL(baseURL);
    	encode2.setNamespace("location", "location.xsd");
    	encode2.encode(xml,  new ListFeatureCollection(TYPE, collection));

    	xml.close();

    	String gml = xml.toString();
    	System.out.println(gml);
    	assertTrue(gml.indexOf("<gml:Point>") != -1);

    }
	@Test
    public void test_encodeGML3() throws Exception {
        
    	SimpleFeatureType TYPE = DataUtilities.createType("location", "geom:Point,name:String");

    	File locationFile = new File("location.xsd");
    	locationFile = locationFile.getCanonicalFile();
    	
        locationFile.deleteOnExit();
        if (locationFile.exists()) {
            locationFile.delete();
        }
        
    	locationFile.createNewFile();

    	URL locationURL = locationFile.toURI().toURL();
    	URL baseURL = locationFile.getParentFile().toURI().toURL();

    	FileOutputStream xsd = new FileOutputStream(locationFile);

    	GML encode = new GML(Version.GML3);
    	encode.setBaseURL(baseURL);
    	encode.setNamespace("location", locationURL.toExternalForm());
    	encode.encode(xsd, TYPE);

    	xsd.close();

    	WKTReader2 wkt = new WKTReader2();
    	List<SimpleFeature> collection = new LinkedList<SimpleFeature>();
    	collection.add(SimpleFeatureBuilder.build(TYPE, new Object[] { wkt.read("POINT (1 2)"),"name1" }, null));
    	collection.add(SimpleFeatureBuilder.build(TYPE, new Object[] { wkt.read("POINT (4 4)"),"name2" }, null));

    	ByteArrayOutputStream xml = new ByteArrayOutputStream();
		
    	GML encode2 = new GML(Version.WFS1_1);
    	encode2.setNamespace("geotools", "http://geotools.org");
    	encode2.encode(xml,  new ListFeatureCollection(TYPE, collection));

    	xml.close();

    	String gml = xml.toString();
    	System.out.println(gml);

    }		
    @Test
    public void test_encode_WFS1_0_From_FeatureCollection() throws Exception {
        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        tb.setName("feature");
        tb.setNamespaceURI("http://geotools.org");
        tb.add("geometry", Point.class);
        tb.add("name", String.class);

        SimpleFeatureType TYPE = tb.buildFeatureType();

        WKTReader2 wkt = new WKTReader2();
        List<SimpleFeature> collection = new LinkedList<SimpleFeature>();
        collection.add(SimpleFeatureBuilder.build(TYPE, new Object[] { wkt.read("POINT (1 2)"),"name1" }, null));
        collection.add(SimpleFeatureBuilder.build(TYPE, new Object[] { wkt.read("POINT (4 4)"),"name2" }, null));
        
        SimpleFeatureCollection featureCollection = new ListFeatureCollection(TYPE, collection);

        
        GML encode = new GML(Version.WFS1_0);
        encode.setNamespace("geotools", "http://geotools.org");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        encode.encode(out, featureCollection);
        out.close();
        String gml = out.toString();
        System.out.println(gml);
        assertTrue(gml.indexOf("<gml:Point>") != -1);
        
		try (FileOutputStream output = new FileOutputStream("C:\\Download\\test_wfs10_gmlv2.gml")) {
			encode.encode(output, featureCollection );
		} catch (IOException e) {
			e.printStackTrace();
		}		
    }
    @Test
    public void test_decode_GML2File_To_FeatureCollection() throws Exception {
        Logger log = org.geotools.util.logging.Logging.getLogger("org.geotools.xml");
        Level level = log.getLevel();
        try {
            log.setLevel( Level.ALL );
            
            File inFile = new File("C:\\Download\\test_wfs10_gmlv2.gml");
            InputStream targetStream = new FileInputStream(inFile);
            
            GML gml = new GML(Version.WFS1_0);
            SimpleFeatureCollection featureCollection = gml.decodeFeatureCollection(targetStream);

            assertNotNull(featureCollection);
            assertEquals(2, featureCollection.size());
            System.out.println(featureCollection.getBounds());
        } finally {
            log.setLevel(level);
        }
    }
	@Test
    public void test_encode_GML3_or_WFS1_1_From_FeatureCollection() throws Exception {
        
        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        tb.setName("feature");
        tb.setNamespaceURI("http://geotools.org");
        tb.add("geometry", Point.class);
        tb.add("name", String.class);

        SimpleFeatureType TYPE = tb.buildFeatureType();

        WKTReader2 wkt = new WKTReader2();
        List<SimpleFeature> collection = new LinkedList<SimpleFeature>();
        collection.add(SimpleFeatureBuilder.build(TYPE, new Object[] { wkt.read("POINT (1 2)"),"name1" }, null));
        collection.add(SimpleFeatureBuilder.build(TYPE, new Object[] { wkt.read("POINT (4 4)"),"name2" }, null));
        
        SimpleFeatureCollection featureCollection = new ListFeatureCollection(TYPE, collection);

        
        GML encode = new GML(Version.WFS1_1);
        encode.setNamespace("geotools", "http://geotools.org");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        encode.encode(out, featureCollection);
        out.close();
        String gml = out.toString();
        System.out.println(gml);
        
		try (FileOutputStream output = new FileOutputStream("C:\\Download\\test_wfs11_gmlv3.gml")) {
			encode.encode(output, featureCollection );
		} catch (IOException e) {
			e.printStackTrace();
		}

    }    
    @Test
    public void test_decode_GML3_or_WFS1_1_File_To_FeatureCollection() throws Exception {
        Logger log = org.geotools.util.logging.Logging.getLogger("org.geotools.xml");
        Level level = log.getLevel();
        try {
            log.setLevel( Level.ALL );
            
            File inFile = new File("C:\\Download\\test_wfs11_gmlv3.gml");
            InputStream targetStream = new FileInputStream(inFile);
            
            GML gml = new GML(Version.WFS1_1);
            SimpleFeatureCollection featureCollection = gml.decodeFeatureCollection(targetStream);

            assertNotNull(featureCollection);
            assertEquals(2, featureCollection.size());
            System.out.println(featureCollection.getBounds());
            
            InputStream in = new FileInputStream(inFile);
            SimpleFeatureIterator iter = gml.decodeFeatureIterator(in);
            while( iter.hasNext() ){
                SimpleFeature f = iter.next();
                System.out.println(f.getID());
                System.out.println(f.getBounds());
             }
            
            
        } finally {
            log.setLevel(level);
        }
    }
}
