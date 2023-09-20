
package com.jdvn.setl.geos.processors.mapviewer;


import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IGeometryFilter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.JtsAdapter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.MvtReader;
import com.wdtinc.mapbox_vector_tile.adapt.jts.TagKeyValueMapConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.TileGeomResult;
import com.wdtinc.mapbox_vector_tile.adapt.jts.UserDataIgnoreConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.model.JtsLayer;
import com.wdtinc.mapbox_vector_tile.adapt.jts.model.JtsMvt;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerBuild;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerParams;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;

public class VectorTilesTest {


    private static final long SEED = 487125064L;
    private static final Random RANDOM = new Random(SEED);
    private static final double WORLD_SIZE = 100D;
    private static final IGeometryFilter ACCEPT_ALL_FILTER = geometry -> true;
    private static final MvtLayerParams DEFAULT_MVT_PARAMS = new MvtLayerParams();
    private static String TEST_LAYER_NAME = "myFeatures";
    
	@Before
	public void init()  {		 
	}
	
	@AfterAll
	public void Cleanup()  {		     
	}
	
    private static CoordinateSequence getCoordSeq(Random random, int pointCount, GeometryFactory geomFactory) {
        final CoordinateSequence coordSeq = geomFactory.getCoordinateSequenceFactory().create(pointCount, 2);
        for(int i = 0; i < pointCount; ++i) {
            final Coordinate coord = coordSeq.getCoordinate(i);
            coord.setOrdinate(0, random.nextDouble() * WORLD_SIZE);
            coord.setOrdinate(1, random.nextDouble() * WORLD_SIZE);
        }
        return coordSeq;
    }    
    private static LineString buildLineString(Random random, int pointCount, GeometryFactory geomFactory) {
        final CoordinateSequence coordSeq = getCoordSeq(random, pointCount, geomFactory);
        return new LineString(coordSeq, geomFactory);
    }
    private static VectorTile.Tile encodeMvt(MvtLayerParams mvtParams, TileGeomResult tileGeom) {
        // Build MVT
        final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
        // Create MVT layer
        final VectorTile.Tile.Layer.Builder layerBuilder = MvtLayerBuild.newLayerBuilder(TEST_LAYER_NAME, mvtParams);
        final MvtLayerProps layerProps = new MvtLayerProps();
        final UserDataIgnoreConverter ignoreUserData = new UserDataIgnoreConverter();
        // MVT tile geometry to MVT features
        final List<VectorTile.Tile.Feature> features = JtsAdapter.toFeatures(tileGeom.mvtGeoms, layerProps, ignoreUserData);
        layerBuilder.addAllFeatures(features);
        MvtLayerBuild.writeProps(layerBuilder, layerProps);
        // Build MVT layer
        final VectorTile.Tile.Layer layer = layerBuilder.build();
        // Add built layer to MVT
        tileBuilder.addLayers(layer);
        /// Build MVT
        return tileBuilder.build();
    }    
    @Test
    public void createVectorTiles() throws IOException{
        // Create input geometry
        final GeometryFactory geomFactory = new GeometryFactory();
        final Geometry inputGeom = buildLineString(RANDOM, 10, geomFactory);
        // Build tile envelope - 1 quadrant of the world
        final Envelope tileEnvelope = new Envelope(0d, WORLD_SIZE * .5d, 0d, WORLD_SIZE * .5d);

        // Build MVT tile geometry
        final TileGeomResult tileGeom = JtsAdapter.createTileGeom(inputGeom, tileEnvelope, geomFactory,DEFAULT_MVT_PARAMS, ACCEPT_ALL_FILTER);
        // Create MVT layer
        final VectorTile.Tile mvt = encodeMvt(DEFAULT_MVT_PARAMS, tileGeom);
        // MVT Bytes
        final byte[] bytes = mvt.toByteArray();

        assertNotNull(bytes);
        
        JtsMvt expected = new JtsMvt(singletonList(new JtsLayer(TEST_LAYER_NAME, tileGeom.mvtGeoms)));

        // Load multipolygon z0 tile
        JtsMvt actual = MvtReader.loadMvt(
                new ByteArrayInputStream(bytes),
                new GeometryFactory(),
                new TagKeyValueMapConverter());

        // Check that MVT geometries are the same as the ones that were encoded above
        assertEquals(expected, actual);
    }    
    
}
