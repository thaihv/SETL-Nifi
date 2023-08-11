
package com.jdvn.setl.geos.processors.rasters;


import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.commons.io.IOUtils;
import org.geotools.data.geojson.GeoJSONReader;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.FeatureLayer;
import org.geotools.map.Layer;
import org.geotools.map.MapContent;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.renderer.GTRenderer;
import org.geotools.renderer.label.LabelCacheImpl;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.styling.AnchorPoint;
import org.geotools.styling.Displacement;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.Fill;
import org.geotools.styling.Graphic;
import org.geotools.styling.LineSymbolizer;
import org.geotools.styling.PointSymbolizer;
import org.geotools.styling.PolygonSymbolizer;
import org.geotools.styling.Rule;
import org.geotools.styling.Stroke;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactory;
import org.junit.Before;
import org.junit.Test;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;
import org.opengis.style.GraphicalSymbol;

public class FeaturesToRastersTest {

	@Before
	public void init()  {

	}
	
	class BoundingBox {
		double north;
		double south;
		double east;
		double west;
	}

	BoundingBox tile2boundingBox(final int x, final int y, final int zoom) {
		BoundingBox bb = new BoundingBox();
		bb.north = tile2lat(y, zoom);
		bb.south = tile2lat(y + 1, zoom);
		bb.west = tile2lon(x, zoom);
		bb.east = tile2lon(x + 1, zoom);
		return bb;
	}

	static double tile2lon(int x, int z) {
		return x / Math.pow(2.0, z) * 360.0 - 180;
	}

	static double tile2lat(int y, int z) {
		double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
		return Math.toDegrees(Math.atan(Math.sinh(n)));
	}

    private byte[] imageFromFeatures(SimpleFeatureCollection fc, ReferencedEnvelope bounds, Style style, int w, int h) {

		BufferedImage image = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		Graphics2D gr = image.createGraphics();
		gr.setComposite(AlphaComposite.Clear);
		gr.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		
		MapContent map = new MapContent();
        Layer layer = new FeatureLayer(fc, style);
        map.addLayer(layer);
        
        Rectangle outputArea = new Rectangle(w, h);    	
        GTRenderer renderer = new StreamingRenderer();
        LabelCacheImpl labelCache = new LabelCacheImpl();
        Map<Object, Object> hints = renderer.getRendererHints();
        
        if (hints == null) {
          hints = new HashMap<>();
        }
        hints.put(StreamingRenderer.LABEL_CACHE_KEY, labelCache);
        renderer.setRendererHints(hints);
        renderer.setMapContent(map);
        renderer.paint(gr, outputArea, bounds);        
        try {
			ImageIO.write(image, "PNG", baos);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        return baos.toByteArray();
    }
    private Style createStyle() {
    	
    	StyleFactory styleFactory = CommonFactoryFinder.getStyleFactory();
    	FilterFactory2 filterFactory = CommonFactoryFinder.getFilterFactory2();
    	
        PolygonSymbolizer polySymbolizer = styleFactory.createPolygonSymbolizer();
        Fill fill = styleFactory.createFill(
                filterFactory.literal("#FFAA00"),
                filterFactory.literal(0.25)
        );
        final Stroke stroke = styleFactory.createStroke(filterFactory.literal(Color.BLACK), filterFactory.literal(2));
        polySymbolizer.setFill(fill);
        polySymbolizer.setStroke(stroke);
        

        Expression opacity = null; // use default
        Expression size = filterFactory.literal(10);
        Expression rotation = null; // use default
        AnchorPoint anchor = null; // use default
        Displacement displacement = null; // use default
        List<GraphicalSymbol> symbols = new ArrayList<>();
        symbols.add(styleFactory.mark(filterFactory.literal("circle"), fill, stroke)); // simple circle backup pla
        // define a point symbolizer of a small circle
        Graphic circle = styleFactory.graphic(symbols, opacity, size, rotation, anchor, displacement);
        PointSymbolizer pointSymbolizer = styleFactory.pointSymbolizer("point", filterFactory.property("geometry"), null, null, circle);
        
        LineSymbolizer lineSymbolizer = styleFactory.createLineSymbolizer();
        lineSymbolizer.setStroke(styleFactory.createStroke(filterFactory.literal(Color.MAGENTA), filterFactory.literal(1)));
        
        Rule rule = styleFactory.createRule();
        rule.symbolizers().add(polySymbolizer);
        rule.symbolizers().add(pointSymbolizer);
        rule.symbolizers().add(lineSymbolizer);
        FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle();
        fts.rules().add(rule);

        Style style = styleFactory.createStyle();
        style.featureTypeStyles().add(fts);
        return style;
        
        
    }
	@Test
    public void test_FeatureCollectionToImages() throws Exception {
        //File inFile = new File("C:\\Download\\test_gjson_poly.json");
        File inFile = new File("C:\\Download\\test_geojson.json");
        InputStream targetStream = new FileInputStream(inFile);        
        GeoJSONReader r = new GeoJSONReader(targetStream);
        SimpleFeatureCollection fc = r.getFeatures();
        r.close();
		try (FileOutputStream output = new FileOutputStream("C:\\Download\\test_featute_image.png")) {
	        Style style = createStyle();
	        //ReferencedEnvelope bounds = fc.getBounds();
	        ReferencedEnvelope envelope = new ReferencedEnvelope(0, 200, 0, 600, DefaultGeographicCRS.WGS84);
	        
	        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(imageFromFeatures(fc, envelope, style, 256, 256));	        
	        IOUtils.copy(byteArrayInputStream, output);			
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        
        
    }
}
