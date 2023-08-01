
package com.jdvn.setl.geos.processors.xml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import org.geotools.kml.KML;
import org.geotools.kml.KMLConfiguration;
import org.geotools.xsd.Encoder;
import org.geotools.xsd.Parser;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class GmlTest {

	@Before
	public void init()  {

	}
    @SuppressWarnings("rawtypes")
	@Test
    public void test_GMLtofile() throws Exception {
        
        File inFile = new File("src/test/resources/xmlfiles/states.kml");
        InputStream targetStream = new FileInputStream(inFile);
      	
        // Read features into kml file
//		StreamingParser st_parser = new StreamingParser(new KMLConfiguration(),targetStream, KML.Placemark);
//		SimpleFeature f = null;
//		while ((f = (SimpleFeature) st_parser.parse()) != null) {
//			FeatureTypeStyle style = (FeatureTypeStyle) f.getAttribute("Style");
//
//			Symbolizer[] syms = style.rules().get(0).getSymbolizers();
//			System.out.println(syms[0].getName());
//
//		}
		
		// Write features into kml file
		Encoder encoder = new Encoder(new KMLConfiguration());
		encoder.setIndenting(true);
		Parser parser = new Parser(new KMLConfiguration());
		SimpleFeature f = (SimpleFeature) parser.parse( targetStream );
		Collection placemarks = (Collection) f.getAttribute("Feature");
		
		try (FileOutputStream output = new FileOutputStream("C:\\Download\\test.kml")) {
			encoder.encode(placemarks, KML.kml, output );
		} catch (IOException e) {
			e.printStackTrace();
		}

    }


}
