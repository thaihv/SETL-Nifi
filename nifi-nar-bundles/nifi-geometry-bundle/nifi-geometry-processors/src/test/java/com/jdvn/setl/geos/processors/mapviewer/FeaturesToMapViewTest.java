
package com.jdvn.setl.geos.processors.mapviewer;


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
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.imageio.ImageIO;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.geotools.TestData;
import org.geotools.data.DefaultRepository;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureWriter;
import org.geotools.data.FileDataStoreFactorySpi;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.gen.PreGeneralizedDataStore;
import org.geotools.data.gen.info.GeneralizationInfos;
import org.geotools.data.gen.info.GeneralizationInfosProvider;
import org.geotools.data.gen.info.GeneralizationInfosProviderImpl;
import org.geotools.data.gen.tool.Toolbox;
import org.geotools.data.geojson.GeoJSONReader;
import org.geotools.data.memory.MemoryDataStore;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeImpl;
import org.geotools.feature.type.GeometryDescriptorImpl;
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
import org.geotools.util.factory.Hints;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;
import org.opengis.style.GraphicalSymbol;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class FeaturesToMapViewTest {

	@SuppressWarnings("rawtypes")
	private static final Cache<CacheKey, FeatureCollection> myCache = Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
	private static final DefaultRepository REPOSITORY = new DefaultRepository();	
	private static Map<Double, Map<String, Integer>> POINTMAP;
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
        @SuppressWarnings("unused")
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
    
    private static MemoryDataStore createMemStoreVertical(
            SimpleFeatureType typ, String name, String fname) throws IOException {
        MemoryDataStore memDS = new MemoryDataStore();

        List<AttributeDescriptor> attrs = new ArrayList<>();
        attrs.addAll(typ.getAttributeDescriptors());
        // have a non linear mapping between source attributes and target attributes
        Collections.shuffle(attrs, new Random(1));

        SimpleFeatureTypeImpl sft =
                new SimpleFeatureTypeImpl(
                        new NameImpl(fname),
                        attrs,
                        typ.getGeometryDescriptor(),
                        typ.isAbstract(),
                        typ.getRestrictions(),
                        typ.getSuper(),
                        typ.getDescription());

        memDS.createSchema(sft);
        REPOSITORY.register(name, memDS);
        return memDS;
    }

    private static MemoryDataStore createMemStoreMixed(
            SimpleFeatureType typ, String name, String fname) throws IOException {
        MemoryDataStore memDS = new MemoryDataStore();
        List<AttributeDescriptor> attrs = new ArrayList<>();
        attrs.addAll(typ.getAttributeDescriptors());
        // have a non linear mapping between source attributes and target attributes
        Collections.shuffle(attrs, new Random(1));

        GeometryDescriptorImpl geom2Descr =
                new GeometryDescriptorImpl(
                        typ.getGeometryDescriptor().getType(),
                        new NameImpl("the_geom2"),
                        typ.getGeometryDescriptor().getMinOccurs(),
                        typ.getGeometryDescriptor().getMaxOccurs(),
                        typ.getGeometryDescriptor().isNillable(),
                        typ.getGeometryDescriptor().getDefaultValue());
        attrs.add(geom2Descr);
        SimpleFeatureTypeImpl sft =
                new SimpleFeatureTypeImpl(
                        new NameImpl(fname),
                        attrs,
                        typ.getGeometryDescriptor(),
                        typ.isAbstract(),
                        typ.getRestrictions(),
                        typ.getSuper(),
                        typ.getDescription());

        memDS.createSchema(sft);
        REPOSITORY.register(name, memDS);
        return memDS;
    }

    private static MemoryDataStore createMemStoreHorizontal(
            SimpleFeatureType typ, String name, String fname) throws IOException {
        MemoryDataStore memDS = new MemoryDataStore();

        List<AttributeDescriptor> attrs = new ArrayList<>();
        attrs.addAll(typ.getAttributeDescriptors());
        // have a non linear mapping between source attributes and target attributes
        Collections.shuffle(attrs, new Random(1));

        GeometryDescriptorImpl geom2Descr =
                new GeometryDescriptorImpl(
                        typ.getGeometryDescriptor().getType(),
                        new NameImpl("the_geom5"),
                        typ.getGeometryDescriptor().getMinOccurs(),
                        typ.getGeometryDescriptor().getMaxOccurs(),
                        typ.getGeometryDescriptor().isNillable(),
                        typ.getGeometryDescriptor().getDefaultValue());
        attrs.add(geom2Descr);
        geom2Descr =
                new GeometryDescriptorImpl(
                        typ.getGeometryDescriptor().getType(),
                        new NameImpl("the_geom10"),
                        typ.getGeometryDescriptor().getMinOccurs(),
                        typ.getGeometryDescriptor().getMaxOccurs(),
                        typ.getGeometryDescriptor().isNillable(),
                        typ.getGeometryDescriptor().getDefaultValue());
        attrs.add(geom2Descr);
        geom2Descr =
                new GeometryDescriptorImpl(
                        typ.getGeometryDescriptor().getType(),
                        new NameImpl("the_geom20"),
                        typ.getGeometryDescriptor().getMinOccurs(),
                        typ.getGeometryDescriptor().getMaxOccurs(),
                        typ.getGeometryDescriptor().isNillable(),
                        typ.getGeometryDescriptor().getDefaultValue());
        attrs.add(geom2Descr);

        geom2Descr =
                new GeometryDescriptorImpl(
                        typ.getGeometryDescriptor().getType(),
                        new NameImpl("the_geom50"),
                        typ.getGeometryDescriptor().getMinOccurs(),
                        typ.getGeometryDescriptor().getMaxOccurs(),
                        typ.getGeometryDescriptor().isNillable(),
                        typ.getGeometryDescriptor().getDefaultValue());
        attrs.add(geom2Descr);

        SimpleFeatureTypeImpl sft =
                new SimpleFeatureTypeImpl(
                        new NameImpl(fname),
                        attrs,
                        typ.getGeometryDescriptor(),
                        typ.isAbstract(),
                        typ.getRestrictions(),
                        typ.getSuper(),
                        typ.getDescription());

        memDS.createSchema(sft);
        REPOSITORY.register(name, memDS);
        return memDS;
    }
    private static void addGeneralizedFeatureVertical(
            SimpleFeature feature, MemoryDataStore memDS, double distance) throws IOException {
        Geometry geomNew =
                TopologyPreservingSimplifier.simplify(
                        (Geometry) feature.getDefaultGeometry(), distance);
        SimpleFeature feature_gen = SimpleFeatureBuilder.deep(feature);
        feature_gen.setDefaultGeometry(geomNew);
        memDS.addFeature(feature_gen);
        POINTMAP.get(distance).put(feature_gen.getID(), (geomNew.getNumPoints()));
    }

    private static void addGeneralizedFeatureMixed(
            SimpleFeature feature, MemoryDataStore memDS, double distance1, double distance2)
            throws IOException {
        SimpleFeature feature_gen2 =
                SimpleFeatureBuilder.template(
                        memDS.getSchema(memDS.getTypeNames()[0]), feature.getID());
        feature_gen2.setAttribute("CAT_ID", feature.getAttribute("CAT_ID"));
        Geometry geomNew =
                TopologyPreservingSimplifier.simplify(
                        (Geometry) feature.getDefaultGeometry(), distance1);
        feature_gen2.setAttribute("the_geom", geomNew);
        geomNew =
                TopologyPreservingSimplifier.simplify(
                        (Geometry) feature.getDefaultGeometry(), distance2);
        feature_gen2.setAttribute("the_geom2", geomNew);
        memDS.addFeature(feature_gen2);
    }

    private static void addGeneralizedFeatureHorizontal(
            SimpleFeature feature, MemoryDataStore memDS) throws IOException {
        SimpleFeature feature_gen2 =
                SimpleFeatureBuilder.template(
                        memDS.getSchema(memDS.getTypeNames()[0]), feature.getID());
        feature_gen2.setAttribute("CAT_ID", feature.getAttribute("CAT_ID"));

        feature_gen2.setAttribute("the_geom", feature.getDefaultGeometry());
        Geometry geomNew =
                TopologyPreservingSimplifier.simplify((Geometry) feature.getDefaultGeometry(), 5);
        feature_gen2.setAttribute("the_geom5", geomNew);
        geomNew =
                TopologyPreservingSimplifier.simplify((Geometry) feature.getDefaultGeometry(), 10);
        feature_gen2.setAttribute("the_geom10", geomNew);
        geomNew =
                TopologyPreservingSimplifier.simplify((Geometry) feature.getDefaultGeometry(), 20);
        feature_gen2.setAttribute("the_geom20", geomNew);
        geomNew =
                TopologyPreservingSimplifier.simplify((Geometry) feature.getDefaultGeometry(), 50);
        feature_gen2.setAttribute("the_geom50", geomNew);
        memDS.addFeature(feature_gen2);

        memDS.addFeature(feature_gen2);
    }
    
    private static void createShapeFilePyramdTestData() throws IOException {

        File baseDir = new File("target" + File.separator + "0");
        if (baseDir.exists() == false) baseDir.mkdir();
        else return; // already done

        // ///////// create property file for streams
        String propFileName =
                "target" + File.separator + "0" + File.separator + "streams.properties";
        File propFile = new File(propFileName);
        try (FileOutputStream out = new FileOutputStream(propFile)) {
            String line = ShapefileDataStoreFactory.URLP.key + "=" + "file:target/0/streams.shp\n";
            out.write(line.getBytes());
        }
        // ////////

        URL url = TestData.url("shapes/streams.shp");

        ShapefileDataStore shapeDS =
                (ShapefileDataStore) new ShapefileDataStoreFactory().createDataStore(url);

        Map<String, Serializable> params = new HashMap<>();
        FileDataStoreFactorySpi factory = new ShapefileDataStoreFactory();
        params.put(
                ShapefileDataStoreFactory.URLP.key,
                new File("target/0/streams.shp").toURI().toURL());
        ShapefileDataStore ds = (ShapefileDataStore) factory.createNewDataStore(params);

        SimpleFeatureSource fs = shapeDS.getFeatureSource(shapeDS.getTypeNames()[0]);

        ds.createSchema(fs.getSchema());
        ds.forceSchemaCRS(fs.getSchema().getCoordinateReferenceSystem());
        try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                        ds.getFeatureWriter(ds.getTypeNames()[0], Transaction.AUTO_COMMIT);
                SimpleFeatureIterator it = fs.getFeatures().features()) {
            while (it.hasNext()) {
                SimpleFeature f = it.next();
                SimpleFeature fNew = writer.next();
                fNew.setAttributes(f.getAttributes());
                writer.write();
            }
        }
        ds.dispose();
        shapeDS.dispose();

        Toolbox tb = new Toolbox();
        tb.parse(
                new String[] {
                    "generalize",
                    "target" + File.separator + "0" + File.separator + "streams.shp",
                    "target",
                    "5.0,10.0,20.0,50.0"
                });
    }    
    private static void createShapeFilePyramd(File file) throws IOException {

    	String fileName = FilenameUtils.removeExtension(file.getName());
  	
        File baseDir = new File("target" + File.separator + "0");
        if (baseDir.exists() == false) baseDir.mkdir();
        else return; // already done

        // ///////// create property file for streams
        String propFileName =
                "target" + File.separator + "0" + File.separator + fileName + ".properties";
        File propFile = new File(propFileName);
        try (FileOutputStream out = new FileOutputStream(propFile)) {
            String line = ShapefileDataStoreFactory.URLP.key + "=" + "file:target/0/" + fileName + ".shp\n";
            out.write(line.getBytes());
        }

        URL url = file.toURI().toURL();

        ShapefileDataStore shapeDS =
                (ShapefileDataStore) new ShapefileDataStoreFactory().createDataStore(url);

        Map<String, Serializable> params = new HashMap<>();
        FileDataStoreFactorySpi factory = new ShapefileDataStoreFactory();
        params.put(
                ShapefileDataStoreFactory.URLP.key,
                new File("target/0/" + fileName + ".shp").toURI().toURL());
        ShapefileDataStore ds = (ShapefileDataStore) factory.createNewDataStore(params);

        SimpleFeatureSource fs = shapeDS.getFeatureSource(shapeDS.getTypeNames()[0]);

        ds.createSchema(fs.getSchema());
        ds.forceSchemaCRS(fs.getSchema().getCoordinateReferenceSystem());
        try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                        ds.getFeatureWriter(ds.getTypeNames()[0], Transaction.AUTO_COMMIT);
                SimpleFeatureIterator it = fs.getFeatures().features()) {
            while (it.hasNext()) {
                SimpleFeature f = it.next();
                SimpleFeature fNew = writer.next();
                fNew.setAttributes(f.getAttributes());
                writer.write();
            }
        }
        ds.dispose();
        shapeDS.dispose();

        Toolbox tb = new Toolbox();
        tb.parse(
                new String[] {
                    "generalize",
                    "target" + File.separator + "0" + File.separator + fileName + ".shp",
                    "target",
                    "1000.0, 5000.0, 10000.0"
                });
    }    
    @Test
    public void pregeneralizedSHPFeatures() throws Exception {    	
    	POINTMAP = new HashMap<>();
        POINTMAP.put(0.0, new HashMap<>());
        POINTMAP.put(5.0, new HashMap<>());
        POINTMAP.put(10.0, new HashMap<>());
        POINTMAP.put(20.0, new HashMap<>());
        POINTMAP.put(50.0, new HashMap<>());
        
        URL url = TestData.url("shapes/streams.shp");

        ShapefileDataStore shp_ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createDataStore(url);
        String typeName = shp_ds.getSchema().getTypeName();
        REPOSITORY.register("dsStreams", shp_ds);
        
        MemoryDataStore dsStreams_5  = createMemStoreVertical(shp_ds.getSchema(), "dsStreams_5", "streams_5");
        MemoryDataStore dsStreams_10 = createMemStoreVertical(shp_ds.getSchema(), "dsStreams_10", "streams_10");
        MemoryDataStore dsStreams_20 = createMemStoreVertical(shp_ds.getSchema(), "dsStreams_20", "streams_20");
        MemoryDataStore dsStreams_50 = createMemStoreVertical(shp_ds.getSchema(), "dsStreams_50", "streams_50");
        
        MemoryDataStore dsStreams_5_10  = createMemStoreMixed(shp_ds.getSchema(), "dsStreams_5_10", "streams_5_10");
        MemoryDataStore dsStreams_20_50 = createMemStoreMixed(shp_ds.getSchema(), "dsStreams_20_50", "streams_20_50");
        MemoryDataStore dsStreams_5_10_20_50 = createMemStoreHorizontal(shp_ds.getSchema(), "dsStreams_5_10_20_50", "streams_5_10_20_50");
        
        Query query = new Query(typeName, Filter.INCLUDE);        
        try (Transaction t = new DefaultTransaction();
                FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                		shp_ds.getFeatureReader(query, t)) {
            while (reader.hasNext()) {
                SimpleFeature stream = reader.next();
                POINTMAP.get(0.0).put(stream.getID(),((Geometry) stream.getDefaultGeometry()).getNumPoints());
                addGeneralizedFeatureVertical(stream, dsStreams_5, 5.0);
                addGeneralizedFeatureVertical(stream, dsStreams_10, 10.0);
                addGeneralizedFeatureVertical(stream, dsStreams_20, 20.0);
                addGeneralizedFeatureVertical(stream, dsStreams_50, 50.0);

                addGeneralizedFeatureMixed(stream, dsStreams_5_10, 5.0, 10.0);
                addGeneralizedFeatureMixed(stream, dsStreams_20_50, 20.0, 50.0);
                addGeneralizedFeatureHorizontal(stream, dsStreams_5_10_20_50);
            }
        }                       
        GeneralizationInfosProvider provider = new GeneralizationInfosProviderImpl();
        GeneralizationInfos ginfos = null;
        PreGeneralizedDataStore ds = null;
        try {
            ginfos = provider.getGeneralizationInfos("src/test/resources/pregeneralized/geninfo_vertical.xml");
            ds = new PreGeneralizedDataStore(ginfos, REPOSITORY);
            typeName = ds.getTypeNames()[0];   
            System.out.println(typeName);
            FeatureSource<SimpleFeatureType, SimpleFeature> fs = ds.getFeatureSource(typeName);
            SimpleFeatureCollection fCollection = (SimpleFeatureCollection) fs.getFeatures();
            Geometry original = null;
            try (SimpleFeatureIterator iterator = fCollection.features()) {
                if (iterator.hasNext()) {
                    original = (Geometry) iterator.next().getDefaultGeometry();
                    System.out.println(original);
                }
            }
            
            Query qGeneralization = new  Query(typeName);
            qGeneralization.getHints().put(Hints.GEOMETRY_DISTANCE, 17.0);
            fCollection = (SimpleFeatureCollection) fs.getFeatures(qGeneralization);
            
            Geometry simplified = null;
            try (SimpleFeatureIterator iterator = (SimpleFeatureIterator) fCollection.features()) {
                if (iterator.hasNext()) {
                    simplified = (Geometry) iterator.next().getDefaultGeometry();
                    System.out.println(simplified);                	
                }
            }
                           
        } catch (IOException ex) {
            java.util.logging.Logger.getGlobal().log(java.util.logging.Level.INFO, "", ex);
            Assert.fail();
        }
    }
    @Test
    public void createPregeneralizedSHPUsingToolBox() throws Exception {
    	createShapeFilePyramdTestData();
    }
    @Test
    public void pregeneralizedFromFeatureCollection() throws Exception {
    	File file = new File("src/test/resources/admzone/SPC_CADAMAP.shp");
    	createShapeFilePyramd(file);

    }       
}
