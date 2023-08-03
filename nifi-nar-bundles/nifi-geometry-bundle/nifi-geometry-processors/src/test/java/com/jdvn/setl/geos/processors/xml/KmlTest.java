
package com.jdvn.setl.geos.processors.xml;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.WKTReader2;
import org.geotools.kml.KML;
import org.geotools.kml.KMLConfiguration;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.Symbolizer;
import org.geotools.xsd.Encoder;
import org.geotools.xsd.Parser;
import org.geotools.xsd.StreamingParser;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class KmlTest {

	@Before
	public void init()  {

	}
	@SuppressWarnings("unchecked")
	@Test
    public void test_KMLtofile() throws Exception {
        
        File inFile = new File("src/test/resources/xmlfiles/states.kml");
        InputStream targetStream = new FileInputStream(inFile);
      	
        // Read features into kml file
		StreamingParser st_parser = new StreamingParser(new KMLConfiguration(),targetStream, KML.Placemark);
		SimpleFeature fea = null;
		Symbolizer[] syms = null;
		while ((fea = (SimpleFeature) st_parser.parse()) != null) {
			FeatureTypeStyle style = (FeatureTypeStyle) fea.getAttribute("Style");
			syms = style.rules().get(0).getSymbolizers();
		}
		assertEquals(3, syms.length);
		
		// Write features into kml file
		InputStream in = new FileInputStream(inFile);
		Encoder encoder = new Encoder(new KMLConfiguration());
		encoder.setIndenting(true);
		
		Parser parser = new Parser(new KMLConfiguration());
		SimpleFeature f = (SimpleFeature) parser.parse( in );
		Collection<SimpleFeature> placemarks = (Collection<SimpleFeature>) f.getAttribute("Feature");
		
		
        Iterator<SimpleFeature> iterator = placemarks.iterator();
        SimpleFeature sf = null;
        while (iterator.hasNext()) {
        	sf = iterator.next();        	
        }
        
        System.out.println("value= " + sf.getBounds());
        
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
        
        
		try (FileOutputStream output = new FileOutputStream("C:\\Download\\test_kmlv1.kml")) {
			encoder.encode(featureCollection, KML.kml, output);
		} catch (IOException e) {
			e.printStackTrace();
		}

    }


}
