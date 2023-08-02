
package com.jdvn.setl.geos.processors.xml;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import org.geotools.kml.KML;
import org.geotools.kml.KMLConfiguration;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.Symbolizer;
import org.geotools.xsd.Encoder;
import org.geotools.xsd.Parser;
import org.geotools.xsd.StreamingParser;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class KmlTest {

	@Before
	public void init()  {

	}
    @SuppressWarnings("rawtypes")
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
		Collection placemarks = (Collection) f.getAttribute("Feature");
		
		try (FileOutputStream output = new FileOutputStream("C:\\Download\\test_kmlv1.kml")) {
			encoder.encode(placemarks, KML.kml, output);
		} catch (IOException e) {
			e.printStackTrace();
		}

    }


}
