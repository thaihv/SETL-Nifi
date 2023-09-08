
package com.jdvn.setl.geos.processors.rasters;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.imageio.ImageIO;

import org.apache.commons.io.IOUtils;
import org.geotools.data.geojson.GeoJSONReader;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;
import org.opengis.style.GraphicalSymbol;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class FeaturesToRastersTest {

	@SuppressWarnings("rawtypes")
	private static final Cache<CacheKey, FeatureCollection> myCache = Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
	
	private final CacheKey key = new CacheKey("my_image");
	
	@Before
	public void init()  {		 
	}
	
	@AfterAll
	public void Cleanup()  {		
        myCache.invalidateAll();        
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
        File inFile = new File("C:\\Download\\test_geojson.json");
        InputStream targetStream = new FileInputStream(inFile);        
        GeoJSONReader r = new GeoJSONReader(targetStream);
        SimpleFeatureCollection fc = r.getFeatures();
        r.close();
               
        myCache.put(key, fc);
		try (FileOutputStream output = new FileOutputStream("C:\\Download\\test_featute_image.png")) {
	        Style style = createStyle();
	        //ReferencedEnvelope bounds = fc.getBounds();
	        ReferencedEnvelope envelope = new ReferencedEnvelope(0, 200, 0, 600, DefaultGeographicCRS.WGS84);
	        
	        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(imageFromFeatures(fc, envelope, style, 256, 256));	   
	        	        
	        IOUtils.copy(byteArrayInputStream, output);			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			SimpleFeatureCollection testfc = (SimpleFeatureCollection) myCache.getIfPresent(key);
			System.out.println("value image 1: " + testfc);
			
			Thread.sleep(2000);
			
			System.out.println("value image 2: " + myCache.getIfPresent(key));
			
			Thread.sleep(1000);
			
			System.out.println("value image 3: " + myCache.getIfPresent(key));
			
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
    }
	
	@Test
    public void get_EnvelopXY(){
		String envelope = "[10, 20, 50, 46]"; 
		envelope = envelope.substring(1, envelope.length() - 1);
		List<String> xy = Arrays.asList(envelope.split(","));
		System.out.println(xy);
		System.out.println(xy.get(0));
		System.out.println(xy.get(2).trim());
		
		
        synchronized (myCache) {
        	SimpleFeatureCollection testfc = (SimpleFeatureCollection) myCache.getIfPresent(key);
        	System.out.println("value image: " + testfc);
        }
        
    }	
    @Test
    public void givenCache_whenPopulate_thenValueStored() {
        Cache<String, DataObject> cache = Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).maximumSize(100).build();

        String key = "A";
        DataObject dataObject = cache.getIfPresent(key);

        assertNull(dataObject);

        dataObject = cache.get(key, k -> DataObject.get("Data for A"));

        assertNotNull(dataObject);
        assertEquals("Data for A", dataObject.getData());

        cache.put(key, dataObject);
        dataObject = cache.getIfPresent(key);

        assertNotNull(dataObject);

        cache.invalidate(key);
        dataObject = cache.getIfPresent(key);

        assertNull(dataObject);
    }

    @Test
    public void givenLoadingCache_whenGet_thenValuePopulated() {
        LoadingCache<String, DataObject> cache = Caffeine.newBuilder().maximumSize(100).expireAfterWrite(1, TimeUnit.MINUTES).build(k -> DataObject.get("Data for " + k));
        String key = "A";

        DataObject dataObject = cache.get(key);

        assertNotNull(dataObject);
        assertEquals("Data for " + key, dataObject.getData());

        Map<String, DataObject> dataObjectMap = cache.getAll(Arrays.asList("A", "B", "C"));

        assertEquals(3, dataObjectMap.size());
    }

    @Test
    public void givenAsyncLoadingCache_whenGet_thenValuePopulated() {

        AsyncLoadingCache<String, DataObject> cache = Caffeine.newBuilder().maximumSize(100).expireAfterWrite(1, TimeUnit.MINUTES).buildAsync(k -> DataObject.get("Data for " + k));
        String key = "A";

        cache.get(key).thenAccept(dataObject -> {
            assertNotNull(dataObject);
            assertEquals("Data for " + key, dataObject.getData());
        });

        cache.getAll(Arrays.asList("A", "B", "C")).thenAccept(dataObjectMap -> Assert.assertEquals(3, dataObjectMap.size()));
    }

    @Test
    public void givenLoadingCacheWithSmallSize_whenPut_thenSizeIsConstant() {
        LoadingCache<String, DataObject> cache = Caffeine.newBuilder().maximumSize(1).refreshAfterWrite(10, TimeUnit.MINUTES).build(k -> DataObject.get("Data for " + k));

        Assert.assertEquals(0, cache.estimatedSize());

        cache.get("A");

        Assert.assertEquals(1, cache.estimatedSize());

        cache.get("B");
        cache.cleanUp();

        Assert.assertEquals(1, cache.estimatedSize());
    }

    @Test
    public void givenLoadingCacheWithWeigher_whenPut_thenSizeIsConstant() {
        LoadingCache<String, DataObject> cache = Caffeine.newBuilder().maximumWeight(10).weigher((k, v) -> 5).build(k -> DataObject.get("Data for " + k));

        Assert.assertEquals(0, cache.estimatedSize());

        cache.get("A");

        Assert.assertEquals(1, cache.estimatedSize());

        cache.get("B");

        Assert.assertEquals(2, cache.estimatedSize());

        cache.get("C");
        cache.cleanUp();

        Assert.assertEquals(2, cache.estimatedSize());
    }

    @Test
    public void givenTimeEvictionCache_whenTimeLeft_thenValueEvicted() {
        LoadingCache<String, DataObject> cache = Caffeine.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).build(k -> DataObject.get("Data for " + k));

        cache = Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.SECONDS).weakKeys().weakValues().build(k -> DataObject.get("Data for " + k));

        cache = Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.SECONDS).softValues().build(k -> DataObject.get("Data for " + k));

        cache = Caffeine.newBuilder().expireAfter(new Expiry<String, DataObject>() {
            @Override
            public long expireAfterCreate(@Nonnull String key, @Nonnull DataObject value, long currentTime) {
                return value.getData().length() * 1000;
            }

            @Override
            public long expireAfterUpdate(@Nonnull String key, @Nonnull DataObject value, long currentTime, long currentDuration) {
                return currentDuration;
            }

            @Override
            public long expireAfterRead(@Nonnull String key, @Nonnull DataObject value, long currentTime, long currentDuration) {
                return currentDuration;
            }
        }).build(k -> DataObject.get("Data for " + k));

        cache = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES).build(k -> DataObject.get("Data for " + k));
    }

    @Test
    public void givenCache_whenStatsEnabled_thenStatsRecorded() {
        LoadingCache<String, DataObject> cache = Caffeine.newBuilder().maximumSize(100).recordStats().build(k -> DataObject.get("Data for " + k));
        cache.get("A");
        cache.get("A");

        Assert.assertEquals(1, cache.stats().hitCount());
        Assert.assertEquals(1, cache.stats().missCount());
    }
}
