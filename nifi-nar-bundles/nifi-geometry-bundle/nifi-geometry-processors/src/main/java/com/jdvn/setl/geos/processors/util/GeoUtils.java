package com.jdvn.setl.geos.processors.util;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.geojson.GeoJSONWriter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geopkg.GeoPackage;
import org.geotools.geopkg.Tile;
import org.geotools.geopkg.TileEntry;
import org.geotools.geopkg.TileMatrix;
import org.geotools.geopkg.TileReader;
import org.geotools.geopkg.geom.GeoPkgGeomReader;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.Id;
import org.opengis.filter.identity.FeatureId;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jdvn.setl.geos.processors.db.LayerMetadata;

public class GeoUtils {

	public static final String GEO_URL = "source.url";
	public static final String SETL_UUID = "NIFIUID";
	public static final String GSS_GEO_COLUMN = "SHAPE";
	public static final String SHP_GEO_COLUMN = "the_geom";
    public static final String GEO_TTLE_MATRIX_BYTES_LEN = "geo.raster.matrixbytes";
    public static final String GEOPACKAGE_CONTENTS = "gpkg_contents";
    public static final String GEOMETRY_COLUMNS = "gpkg_geometry_columns";
    public static final String SPATIAL_REF_SYS = "gpkg_spatial_ref_sys";
    public static final String GEO_CHAR_SET = "geo.source.charset";
    public static final String GPKG_TILE_ZOOM = "zoom_level";    
    public static final String GPKG_TILE_COLUMN = "tile_column";
    public static final String GPKG_TILE_ROW = "tile_row";
    public static final String GPKG_TILE_DATA = "tile_data";
    public static final String GEO_DB_SRC_TYPE = "geo.source.type";
    
    public static final String DATA_COLUMNS = "gpkg_data_columns";
    protected static final int GENERIC_GEOGRAPHIC_SRID = 0;
    protected static final int GENERIC_PROJECTED_SRID = -1;
    static final String DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    
	private static final Logger logger = LoggerFactory.getLogger(GeoUtils.class);

	private static SimpleFeatureType generateFeatureType(final String typeName, final CoordinateReferenceSystem crs,
			final String geometryName, final Class<? extends Geometry> geometryClass,
			final Map<String, Class<?>> attributes) {
		final SimpleFeatureTypeBuilder featureTypeBuilder = new SimpleFeatureTypeBuilder();
		featureTypeBuilder.setName(typeName);
		featureTypeBuilder.setCRS(crs);
		featureTypeBuilder.add(geometryName, geometryClass);

		if (attributes != null) {
			attributes.forEach(featureTypeBuilder::add);
		}
		return featureTypeBuilder.buildFeatureType();
	}

	public static String getGeometryFieldName(Record record) {
		String geoKey = null;
		for (int i = 0; i < record.getSchema().getFieldCount(); i++) {
			String value = record.getAsString(record.getSchema().getFields().get(i).getFieldName());
			if ((value != null) && (value.contains("MULTILINESTRING") || value.contains("LINESTRING") || value.contains("MULTIPOLYGON")
					|| value.contains("POLYGON") || value.contains("POINT") || value.contains("MULTIPOINT")
					|| value.contains("GEOMETRYCOLLECTION"))) {

				geoKey = record.getSchema().getFields().get(i).getFieldName();
				break;
			}
		}
		return geoKey;

	}

	public static List<FeatureId> getFeatureIds(SimpleFeatureCollection features) {
		List<FeatureId> featureIds = new ArrayList<FeatureId>();

		SimpleFeatureIterator it = features.features();
		try {
			while (it.hasNext()) {
				SimpleFeature feature = it.next();
				featureIds.add(feature.getIdentifier());
			}

		} finally {
			it.close();
		}
		return featureIds;
	}
	public static RecordSchema createFeatureRecordSchema(SimpleFeatureSource featureSource) {
		SimpleFeatureType schema = featureSource.getSchema();
		final List<RecordField> fields = new ArrayList<>();
		boolean hasIDField = false;
		for (int i = 0; i < schema.getAttributeCount(); i++) {
			String fieldName = schema.getDescriptor(i).getName().getLocalPart();
			if (fieldName.toUpperCase().equals(GeoUtils.SETL_UUID))
				hasIDField = true;
			String fieldType = schema.getDescriptor(i).getType().getBinding().getSimpleName();
			DataType dataType;
			switch (fieldType) {
			case "Long":
				dataType = RecordFieldType.LONG.getDataType();
				break;
			case "String":
				dataType = RecordFieldType.STRING.getDataType();
				break;
			case "Double":
				dataType = RecordFieldType.DOUBLE.getDataType();
				break;
			case "Boolean":
				dataType = RecordFieldType.BOOLEAN.getDataType();
				break;
			case "Byte":
				dataType = RecordFieldType.BYTE.getDataType();
				break;
			case "Character":
				dataType = RecordFieldType.CHAR.getDataType();
				break;
			case "Integer":
				dataType = RecordFieldType.INT.getDataType();
				break;
			case "Float":
				dataType = RecordFieldType.FLOAT.getDataType();
				break;
			case "Number":
				dataType = RecordFieldType.BIGINT.getDataType();
				break;
			case "Date":
				dataType = RecordFieldType.DATE.getDataType();
				break;
			case "Time":
				dataType = RecordFieldType.TIME.getDataType();
				break;
			case "Timestamp":
				dataType = RecordFieldType.TIMESTAMP.getDataType();
				break;
			case "Short":
				dataType = RecordFieldType.SHORT.getDataType();
				break;
			default:
				dataType = RecordFieldType.STRING.getDataType();
			}
			fields.add(new RecordField(fieldName, dataType));
		}
		if (!hasIDField)
			fields.add(new RecordField(GeoUtils.SETL_UUID, RecordFieldType.STRING.getDataType()));
		
		RecordSchema recordSchema = new SimpleRecordSchema(fields);
		return recordSchema;
	}	
	
	public static RecordSchema createFeatureRecordSchema(SimpleFeatureCollection collection) {
		SimpleFeatureType schema = null;
		if (collection.features().hasNext())
			schema = collection.features().next().getFeatureType();
		else
			return null;
		
		final List<RecordField> fields = new ArrayList<>();
		boolean hasIDField = false;
		for (int i = 0; i < schema.getAttributeCount(); i++) {
			String fieldName = schema.getDescriptor(i).getName().getLocalPart();
			if (fieldName.toUpperCase().equals(GeoUtils.SETL_UUID))
				hasIDField = true;
			String fieldType = schema.getDescriptor(i).getType().getBinding().getSimpleName();
			DataType dataType;
			switch (fieldType) {
			case "Long":
				dataType = RecordFieldType.LONG.getDataType();
				break;
			case "String":
				dataType = RecordFieldType.STRING.getDataType();
				break;
			case "Double":
				dataType = RecordFieldType.DOUBLE.getDataType();
				break;
			case "Boolean":
				dataType = RecordFieldType.BOOLEAN.getDataType();
				break;
			case "Byte":
				dataType = RecordFieldType.BYTE.getDataType();
				break;
			case "Character":
				dataType = RecordFieldType.CHAR.getDataType();
				break;
			case "Integer":
				dataType = RecordFieldType.INT.getDataType();
				break;
			case "Float":
				dataType = RecordFieldType.FLOAT.getDataType();
				break;
			case "Number":
				dataType = RecordFieldType.BIGINT.getDataType();
				break;
			case "Date":
				dataType = RecordFieldType.DATE.getDataType();
				break;
			case "Time":
				dataType = RecordFieldType.TIME.getDataType();
				break;
			case "Timestamp":
				dataType = RecordFieldType.TIMESTAMP.getDataType();
				break;
			case "Short":
				dataType = RecordFieldType.SHORT.getDataType();
				break;
			default:
				dataType = RecordFieldType.STRING.getDataType();
			}
			fields.add(new RecordField(fieldName, dataType));
		}
		if (!hasIDField)
			fields.add(new RecordField(GeoUtils.SETL_UUID, RecordFieldType.STRING.getDataType()));
		
		RecordSchema recordSchema = new SimpleRecordSchema(fields);
		return recordSchema;
	}
	
	public static ArrayList<Record> getNifiRecordsFromFeatureSource(final SimpleFeatureSource featureSource, Charset charset) {
		final ArrayList<Record> returnRs = new ArrayList<Record>();
		try {
			final RecordSchema recordSchema = createFeatureRecordSchema(featureSource);
			SimpleFeatureCollection features = featureSource.getFeatures();	
			SimpleFeatureIterator it = (SimpleFeatureIterator) features.features();
			while (it.hasNext()) {
				SimpleFeature feature = it.next();
				Map<String, Object> fieldMap = new HashMap<String, Object>();
				for (int i = 0; i < feature.getAttributeCount(); i++) {
					String key = feature.getFeatureType().getDescriptor(i).getName().getLocalPart();
					Object value = feature.getAttribute(i);
					fieldMap.put(key, value);						
				}
				if (feature.getAttribute(GeoUtils.SETL_UUID) == null)
					fieldMap.put(GeoUtils.SETL_UUID, feature.getID());
				Record r = new MapRecord(recordSchema, fieldMap);
				returnRs.add(r);
			}
			it.close();
			return returnRs;

		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}
		return returnRs;
	}
	public static ArrayList<Record> getNifiRecordsFromFeatureCollection(final SimpleFeatureCollection features) {
		final ArrayList<Record> returnRs = new ArrayList<Record>();
		final RecordSchema recordSchema = createFeatureRecordSchema(features);
		SimpleFeatureIterator it = (SimpleFeatureIterator) features.features();
		while (it.hasNext()) {
			SimpleFeature feature = it.next();
			Map<String, Object> fieldMap = new HashMap<String, Object>();
			for (int i = 0; i < feature.getAttributeCount(); i++) {
				String key = feature.getFeatureType().getDescriptor(i).getName().getLocalPart();
				Object value = feature.getAttribute(i);
				fieldMap.put(key, value);						
			}
			if (feature.getAttribute(GeoUtils.SETL_UUID) == null)
				fieldMap.put(GeoUtils.SETL_UUID, feature.getID());
			Record r = new MapRecord(recordSchema, fieldMap);
			returnRs.add(r);
		}
		it.close();
		return returnRs;
	}	
	
	public static String getGeojsonFromFeatureCollection(SimpleFeatureCollection fc) {
	    return GeoJSONWriter.toGeoJSON(fc);
	}	
	public static ArrayList<Record> getNifiRecordSegmentsFromFeatureSource(final SimpleFeatureSource featureSource, final RecordSchema recordSchema, Set<FeatureId> featureIds, Charset charset ) {
		final ArrayList<Record> returnRs = new ArrayList<Record>();
		try {
			FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
			Id fids = ff.id(featureIds);
			
			SimpleFeatureCollection selectedfeatures;
			if (fids != null)
				selectedfeatures = featureSource.getFeatures(fids);
			else
				selectedfeatures = featureSource.getFeatures();
	        	
			SimpleFeatureIterator it = (SimpleFeatureIterator) selectedfeatures.features();
			
			while (it.hasNext()) {
				SimpleFeature feature = it.next();
				Map<String, Object> fieldMap = new HashMap<String, Object>();
				for (int i = 0; i < feature.getAttributeCount(); i++) {
					String key = feature.getFeatureType().getDescriptor(i).getName().getLocalPart();
					Object value = feature.getAttribute(i);
					fieldMap.put(key, value);						
				}
				if (feature.getAttribute(GeoUtils.SETL_UUID) == null)
					fieldMap.put(GeoUtils.SETL_UUID, feature.getID());
				Record r = new MapRecord(recordSchema, fieldMap);
				returnRs.add(r);
			}
			it.close();
			return returnRs;

		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}
		return returnRs;
	}
	public static CoordinateReferenceSystem getCRSFromShapeFile(final File shpFile) {
		Map<String, Object> mapAttrs = new HashMap<>();
		CoordinateReferenceSystem cRS = null;
		try {
			mapAttrs.put("url", shpFile.toURI().toURL());
			DataStore dataStore = DataStoreFinder.getDataStore(mapAttrs);
			String typeName = dataStore.getTypeNames()[0];

			SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeName);

			SimpleFeatureType schema = featureSource.getSchema();
			cRS = schema.getCoordinateReferenceSystem();
			dataStore.dispose();
		} catch (IOException e) {

			e.printStackTrace();
		}

		return cRS;
	}

	public static Coordinate transformCoordinateBasedOnCrs(CoordinateReferenceSystem sourceCRS,
			CoordinateReferenceSystem targetCRS, Coordinate in) {
		Coordinate out = in;

		try {
			MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);
			JTS.transform(in, out, transform);
		} catch (TransformException | FactoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return out;

	}

	public static Map<String, Class<?>> createAttributeTableFromRecordSet(RecordReader avroReader,
			String geomFieldName) {
		Map<String, Class<?>> attributes = new HashMap<>();
		try {
			List<RecordField> fields = avroReader.getSchema().getFields();
			for (int i = 0; i < fields.size(); i++) {
				RecordField f = fields.get(i);
				DataType type = f.getDataType();
				Class<?> obj;
				switch (type.getFieldType()) {
				case LONG:
					obj = Long.class;
					break;
				case STRING:
					obj = String.class;
					break;
				case DOUBLE:
					obj = Double.class;
					break;
				case BOOLEAN:
					obj = Boolean.class;
					break;
				case BYTE:
					obj = Byte.class;
					break;
				case CHAR:
					obj = Character.class;
					break;
				case INT:
					obj = Integer.class;
					break;
				case FLOAT:
					obj = Float.class;
					break;
				case BIGINT:
					obj = Double.class;
					break;
				case DATE:
					obj = Date.class;
					break;
				case TIME:
					obj = Time.class;
					break;
				case TIMESTAMP:
					obj = Timestamp.class;
					break;
				case SHORT:
					obj = Short.class;
					break;
				default:
					obj = String.class;
				}
				attributes.put(f.getFieldName(), obj);
				if (f.getFieldName().contains(geomFieldName)) {
					attributes.remove(geomFieldName);
				}
			}
		} catch (MalformedRecordException e) {
			e.printStackTrace();
		}
		return attributes;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static SimpleFeatureCollection createSimpleFeatureCollectionFromNifiRecords(String collectionName, RecordReader avroReader, CoordinateReferenceSystem crs_source, CoordinateReferenceSystem crs_target) {
		List<SimpleFeature> features = new ArrayList<>();
		String geomFieldName = SHP_GEO_COLUMN;
		Record record;
		try {
			boolean bCreatedSchema = false;
			SimpleFeatureBuilder featureBuilder = null;
			SimpleFeatureType TYPE = null;
			Class geometryClass = null;
			while ((record = avroReader.nextRecord()) != null) {
				if (!bCreatedSchema) {
					geomFieldName = getGeometryFieldName(record);
					String geovalue = record.getAsString(geomFieldName);
					String type = geovalue.substring(0, geovalue.indexOf('(')).toUpperCase().trim();
					switch (type) {
					case "MULTILINESTRING":
						geometryClass = MultiLineString.class;
						break;
					case "LINESTRING":
						geometryClass = LineString.class;
						break;
					case "MULTIPOLYGON":
						geometryClass = MultiPolygon.class;
						break;
					case "POLYGON":
						geometryClass = Polygon.class;
						break;
					case "MULTIPOINT":
						geometryClass = MultiPoint.class;
						break;
					case "POINT":
						geometryClass = Point.class;
						break;
					case "GEOMETRYCOLLECTION":
						geometryClass = GeometryCollection.class;
						break;
					default:
						geometryClass = MultiLineString.class;
					}

					Map<String, Class<?>> attributes = createAttributeTableFromRecordSet(avroReader, geomFieldName);
					// shp file with geo column is "the_geom"
					if (crs_target == null)
						TYPE = generateFeatureType(collectionName, crs_source, SHP_GEO_COLUMN, geometryClass, attributes);
					else
						TYPE = generateFeatureType(collectionName, crs_target, SHP_GEO_COLUMN, geometryClass, attributes);
					featureBuilder = new SimpleFeatureBuilder(TYPE);
					bCreatedSchema = true;
				}
				GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
				WKTReader reader = new WKTReader(geometryFactory);
				// Add geometry
				Geometry geo = reader.read(record.getAsString(geomFieldName));
				if (crs_target != null && crs_target != crs_source) {
			        MathTransform transform = CRS.findMathTransform(crs_source, crs_target);
			        geo = JTS.transform(geo, transform);									
				}
				// Add attributes
				int size = record.getSchema().getFieldCount();
				Object[] objs = new Object[size];
				for (int i = 0; i < size; i++) {
					String fName = record.getSchema().getFieldNames().get(i);
					if ((fName == geomFieldName) && (geomFieldName != SHP_GEO_COLUMN))
						fName = SHP_GEO_COLUMN;
					int index = featureBuilder.getFeatureType().indexOf(fName);
					if (fName.equals(geomFieldName.toLowerCase()) || fName.equals(geomFieldName.toUpperCase()) || fName.equals(SHP_GEO_COLUMN))
						objs[index] = geo;
					else
						objs[index] = record.getValue(fName);

				}
				featureBuilder.addAll(objs);
				SimpleFeature feature = featureBuilder.buildFeature(null);
				features.add(feature);
			}

			return new ListFeatureCollection(TYPE, features);
		} catch (IOException | MalformedRecordException | ParseException | FactoryException | MismatchedDimensionException | TransformException e) {
			logger.error("Could not create SimpleFeatureCollection because {}", new Object[] { e });
		}
		return null;
	}
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static SimpleFeatureCollection createSimpleFeatureCollectionFromNifiRecordsWithoutGeoFname(String collectionName, RecordReader avroReader, CoordinateReferenceSystem crs_source) {
		List<SimpleFeature> features = new ArrayList<>();
		String geomFieldName = SHP_GEO_COLUMN;
		Record record;
		try {
			boolean bCreatedSchema = false;
			SimpleFeatureBuilder featureBuilder = null;
			SimpleFeatureType TYPE = null;
			Class geometryClass = null;
			while ((record = avroReader.nextRecord()) != null) {
				if (!bCreatedSchema) {
					geomFieldName = getGeometryFieldName(record);
					String geovalue = record.getAsString(geomFieldName);
					String type = geovalue.substring(0, geovalue.indexOf('(')).toUpperCase().trim();
					switch (type) {
					case "MULTILINESTRING":
						geometryClass = MultiLineString.class;
						break;
					case "LINESTRING":
						geometryClass = LineString.class;
						break;
					case "MULTIPOLYGON":
						geometryClass = MultiPolygon.class;
						break;
					case "POLYGON":
						geometryClass = Polygon.class;
						break;
					case "MULTIPOINT":
						geometryClass = MultiPoint.class;
						break;
					case "POINT":
						geometryClass = Point.class;
						break;
					case "GEOMETRYCOLLECTION":
						geometryClass = GeometryCollection.class;
						break;
					default:
						geometryClass = MultiLineString.class;
					}

					Map<String, Class<?>> attributes = createAttributeTableFromRecordSet(avroReader, geomFieldName);
					TYPE = generateFeatureType(collectionName, crs_source, geomFieldName, geometryClass, attributes);
					featureBuilder = new SimpleFeatureBuilder(TYPE);
					bCreatedSchema = true;
				}
				GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
				WKTReader reader = new WKTReader(geometryFactory);
				// Add geometry
				Geometry geo = reader.read(record.getAsString(geomFieldName));
				// Add attributes
				int size = record.getSchema().getFieldCount();
				Object[] objs = new Object[size];
				for (int i = 0; i < size; i++) {
					String fName = record.getSchema().getFieldNames().get(i);
					int index = featureBuilder.getFeatureType().indexOf(fName);
					if (fName.equals(geomFieldName.toLowerCase()) || fName.equals(geomFieldName.toUpperCase()))
						objs[index] = geo;
					else
						objs[index] = record.getValue(fName);

				}
				featureBuilder.addAll(objs);
				SimpleFeature feature = featureBuilder.buildFeature(null);
				features.add(feature);
			}

			return new ListFeatureCollection(TYPE, features);
		} catch (IOException | MalformedRecordException | ParseException  | MismatchedDimensionException  e) {
			logger.error("Could not create SimpleFeatureCollection because {}", new Object[] { e });
		}
		return null;
	}	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static SimpleFeatureCollection createNifiRecordsWithCRSTransformed(String collectionName, RecordReader avroReader, CoordinateReferenceSystem crs_source, CoordinateReferenceSystem crs_target) {
		List<SimpleFeature> features = new ArrayList<>();
		String geomFieldName = SHP_GEO_COLUMN;
		Record record;
		try {
			boolean bCreatedSchema = false;
			SimpleFeatureBuilder featureBuilder = null;
			SimpleFeatureType TYPE = null;
			Class geometryClass = null;
			while ((record = avroReader.nextRecord()) != null) {
				if (!bCreatedSchema) {
					geomFieldName = getGeometryFieldName(record);
					String geovalue = record.getAsString(geomFieldName);
					String type = geovalue.substring(0, geovalue.indexOf('(')).toUpperCase().trim();
					switch (type) {
					case "MULTILINESTRING":
						geometryClass = MultiLineString.class;
						break;
					case "LINESTRING":
						geometryClass = LineString.class;
						break;
					case "MULTIPOLYGON":
						geometryClass = MultiLineString.class;
						break;
					case "POLYGON":
						geometryClass = Polygon.class;
						break;
					case "MULTIPOINT":
						geometryClass = MultiPoint.class;
						break;
					case "POINT":
						geometryClass = Point.class;
						break;
					case "GEOMETRYCOLLECTION":
						geometryClass = GeometryCollection.class;
						break;
					default:
						geometryClass = MultiLineString.class;
					}

					Map<String, Class<?>> attributes = createAttributeTableFromRecordSet(avroReader, geomFieldName);
					// shp file with geo column is "the_geom"
					if (crs_target == null)
						TYPE = generateFeatureType(collectionName, crs_source, SHP_GEO_COLUMN, geometryClass, attributes);
					else
						TYPE = generateFeatureType(collectionName, crs_target, SHP_GEO_COLUMN, geometryClass, attributes);
					featureBuilder = new SimpleFeatureBuilder(TYPE);
					bCreatedSchema = true;
				}
				GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
				WKTReader reader = new WKTReader(geometryFactory);
				// Add geometry
				Geometry geo = reader.read(record.getAsString(geomFieldName));
				if (crs_target != null && crs_target != crs_source) {
			        MathTransform transform = CRS.findMathTransform(crs_source, crs_target);
			        geo = JTS.transform(geo, transform);									
				}
				// Add attributes
				int size = record.getSchema().getFieldCount();
				Object[] objs = new Object[size];
				for (int i = 0; i < size; i++) {
					String fName = record.getSchema().getFieldNames().get(i);
					if ((fName == geomFieldName) && (geomFieldName != SHP_GEO_COLUMN))
						fName = SHP_GEO_COLUMN;
					int index = featureBuilder.getFeatureType().indexOf(fName);
					if (fName.contains(geomFieldName) || fName.contains(SHP_GEO_COLUMN))
						objs[index] = geo;
					else
						objs[index] = record.getValue(fName);

				}
				featureBuilder.addAll(objs);
				SimpleFeature feature = featureBuilder.buildFeature(null);
				features.add(feature);
			}

			return new ListFeatureCollection(TYPE, features);
		} catch (IOException | MalformedRecordException | ParseException | FactoryException | MismatchedDimensionException | TransformException e) {
			logger.error("Could not create SimpleFeatureCollection because {}", new Object[] { e });
		}
		return null;
	}
	public static RecordSchema createTileRecordSchema(final TileEntry tileEntry) {
		
		final List<Field> tileFields = new ArrayList<>();
		tileFields.add(new Field(GPKG_TILE_ZOOM, Schema.create(Type.INT), null, (Object) null));
		tileFields.add(new Field(GPKG_TILE_COLUMN, Schema.create(Type.INT), null, (Object) null));
		tileFields.add(new Field(GPKG_TILE_ROW, Schema.create(Type.INT), null, (Object) null));
		tileFields.add(new Field(GPKG_TILE_DATA, Schema.create(Type.BYTES), null, (Object) null));
		final Schema schema = Schema.createRecord(tileEntry.getTableName(), null, null, false);
		schema.setFields(tileFields);
		
		return AvroTypeUtil.createSchema(schema);
	}	
	public static ArrayList<Record> getNifiRecordsFromTileEntry(final GeoPackage geoPackage, final TileEntry tileEntry) {
		final ArrayList<Record> returnRs = new ArrayList<Record>();
		
		RecordSchema schema = createTileRecordSchema(tileEntry);
		try (TileReader r = geoPackage.reader(tileEntry, null, null, null, null, null, null)) {
			while (r.hasNext()) {
				Tile tile = r.next();

				Map<String, Object> fieldMap = new HashMap<String, Object>();
				fieldMap.put(GPKG_TILE_ZOOM, tile.getZoom());
				fieldMap.put(GPKG_TILE_COLUMN, tile.getColumn());
				fieldMap.put(GPKG_TILE_ROW, tile.getRow());
				fieldMap.put(GPKG_TILE_DATA, tile.getData());

				Record tileRecord = new MapRecord(schema, fieldMap);
				returnRs.add(tileRecord);
			}
			r.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return returnRs;
	}
	public static int getTileRecordCount(final GeoPackage geoPackage, final TileEntry tileEntry) {

		int count = 0;
		try (TileReader r = geoPackage.reader(tileEntry, null, null, null, null, null, null)) {
			
			while (r.hasNext()) {
				Tile tile = r.next();
				if (tile != null)
					count++;
			}
			r.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return count;
	}	
	public static ArrayList<Record> getNifiRecordSegmentsFromTileEntry(final GeoPackage geoPackage, final TileEntry tileEntry, final int recordFrom, final int recordTo ) {
		final ArrayList<Record> returnRs = new ArrayList<Record>();
		RecordSchema recordSchema = createTileRecordSchema(tileEntry);
		
		StringBuffer sql = new StringBuffer("SELECT rowid as ROWID, * FROM ");
		sql.append(tileEntry.getTableName());
		sql.append(" WHERE rowid >= ").append(Integer.toString(recordFrom));
		sql.append(" AND ").append(" rowid < ").append(Integer.toString(recordTo));		
		try {
			final DataSource connPool = geoPackage.getDataSource();
			Connection cx = connPool.getConnection();
			PreparedStatement ps = cx.prepareStatement(sql.toString());
            try (ResultSet rs = ps.executeQuery()) {
            	while (rs.next()) {
            		Map<String, Object> fieldMap = new HashMap<String, Object>();
                    for (final RecordField field : recordSchema.getFields()) {
                        final String fieldName = field.getFieldName();
                        String key = fieldName;
                        final Object value = rs.getObject(fieldName);
    					fieldMap.put(key, value);
                    }
    				Record r = new MapRecord(recordSchema, fieldMap);
    				returnRs.add(r);                    
            	}
            	rs.close();
            	return returnRs;
            } catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				ps.close();
				cx.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return returnRs;
	}	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<Tile> getTilesFromNifiRecords(final String entryName, final RecordReader avroReader, CoordinateReferenceSystem crs_source) {
		List<Tile> tiles = new ArrayList();
		Record record;
		try {
			while ((record = avroReader.nextRecord()) != null) {
				int zoom      = record.getAsInt(GPKG_TILE_ZOOM);
				int column    = record.getAsInt(GPKG_TILE_COLUMN);
				int row       = record.getAsInt(GPKG_TILE_ROW);
				ByteBuffer bb = AvroTypeUtil.convertByteArray((Object[]) record.getValue(GPKG_TILE_DATA));
				tiles.add(new Tile(zoom,column,row, bb.array()));
			}
		} catch (IOException | MalformedRecordException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return tiles;
	}	
	public static int[] getMinMaxTilesZoomTileEntry(final GeoPackage geoPackage, TileEntry tileEntry) {

		int minMax[] = { 1, 20};
	    int max = Integer.MIN_VALUE;
	    int min = Integer.MAX_VALUE;
		try (TileReader r = geoPackage.reader(tileEntry, null, null, null, null, null, null)) {
			
			while (r.hasNext()) {
				Tile tile = r.next();

				if (tile.getZoom() > max) {
					max = tile.getZoom();
				}
				if (tile.getZoom() < min) {
					min = tile.getZoom();
				}
			}
			r.close();
			minMax[0] = min;
			minMax[1] = max;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return minMax;
	}
	public static boolean hasColumn(ResultSetMetaData rsmd, String columnName) throws SQLException {
	    int columns = rsmd.getColumnCount();
	    for (int x = 1; x <= columns; x++) {
	        if (columnName.toUpperCase().equals(rsmd.getColumnName(x))) {
	            return true;
	        }
	    }
	    return false;
	}	
	public static ArrayList<Record> getNifiRecordsFromGeoPackageFeatureTable(final SimpleFeatureSource featureSource, final String tableName, final RecordSchema recordSchema) {
		final ArrayList<Record> returnRs = new ArrayList<Record>();
		try {
			SimpleFeatureCollection	selectedfeatures = featureSource.getFeatures();
			SimpleFeatureIterator it = (SimpleFeatureIterator) selectedfeatures.features();
			
			while (it.hasNext()) {
				SimpleFeature feature = it.next();
				Map<String, Object> fieldMap = new HashMap<String, Object>();
				for (int i = 0; i < feature.getAttributeCount(); i++) {
					String key = feature.getFeatureType().getDescriptor(i).getName().getLocalPart();
					Object value = feature.getAttribute(i);
					fieldMap.put(key, value);
				}
				if (feature.getAttribute(GeoUtils.SETL_UUID) == null)
					fieldMap.put(GeoUtils.SETL_UUID, feature.getID());
				Record r = new MapRecord(recordSchema, fieldMap);
				returnRs.add(r);
			}
			it.close();
			return returnRs;

		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}
		return returnRs;
	}	
	public static ArrayList<Record> getNifiRecordSegmentsFromGeoPackageFeatureTable(final File file, final String tableName, final String geofieldName, final RecordSchema recordSchema, final int recordFrom, final int recordTo ) {
		final ArrayList<Record> returnRs = new ArrayList<Record>();
		
		StringBuffer sql = new StringBuffer("SELECT rowid as ROWID, * FROM ");
		sql.append(tableName);
		sql.append(" WHERE rowid >= ").append(Integer.toString(recordFrom));
		sql.append(" AND ").append(" rowid < ").append(Integer.toString(recordTo));		
		GeoPackage geoPackage = null;
		try {
			geoPackage = new GeoPackage(file);
			final DataSource connPool = geoPackage.getDataSource();
			Connection cx = connPool.getConnection();
			PreparedStatement ps = cx.prepareStatement(sql.toString());
            try (ResultSet rs = ps.executeQuery()) {
            	while (rs.next()) {
            		Map<String, Object> fieldMap = new HashMap<String, Object>();
                    for (final RecordField field : recordSchema.getFields()) {
                        final String fieldName = field.getFieldName();
                        String key = fieldName;
                        final Object value;
                        if (fieldName.toUpperCase().equals(geofieldName.toUpperCase())) {
                        	GeoPkgGeomReader reader = new GeoPkgGeomReader(rs.getBytes(fieldName));
                        	Geometry g = reader.get();
        					value = new org.locationtech.jts.io.WKTWriter().write(g);    
                        }
                        else 
                        	if (fieldName.toUpperCase().equals(GeoUtils.SETL_UUID) && !GeoUtils.hasColumn(rs.getMetaData(), GeoUtils.SETL_UUID)){
                        		break;
                        }else
                        	value = rs.getObject(fieldName);
    					fieldMap.put(key, value);
                    }
    				if (!GeoUtils.hasColumn(rs.getMetaData(), GeoUtils.SETL_UUID))
    					fieldMap.put(GeoUtils.SETL_UUID, rs.getString("ROWID"));
    				Record r = new MapRecord(recordSchema, fieldMap);
    				returnRs.add(r);                    
                    
            	}
            	rs.close();
            	return returnRs;
            } catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally {
				ps.close();
				cx.close();
			}
		} catch (IOException | SQLException e) {
			e.printStackTrace();
		} finally {
			if (geoPackage!= null)
				geoPackage.close();
		}
		return returnRs;
	}
	public static CoordinateReferenceSystem getCRSFromGeoPackageFeatureTable(DataStore store, String tableName) {
		CoordinateReferenceSystem cRS = null;
		try {
			SimpleFeatureCollection features = store.getFeatureSource(tableName).getFeatures();
			SimpleFeatureType schema = features.getSchema();
			cRS = schema.getCoordinateReferenceSystem();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return cRS;
	}

	public static CoordinateReferenceSystem getCRSFromGeoPackageTilesTable(final File geopkg, TileEntry tileEntry) {
		CoordinateReferenceSystem cRS = null;
		org.geotools.geopkg.mosaic.GeoPackageReader reader;
		try {
			reader = new org.geotools.geopkg.mosaic.GeoPackageReader(geopkg, null);
			cRS = reader.getCoordinateReferenceSystem(tileEntry.getTableName());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return cRS;
	}
	public static LayerMetadata getLayerMetadata(int layerId, Statement stmt) throws SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT * FROM GSS.THEMES ");
		sb.append("WHERE THEME_ID=").append(layerId);
		
		return getLayerMetadata(sb, stmt);
	}
	public static LayerMetadata getLayerMetadata(String username, String name, Statement stmt) throws SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT * FROM GSS.THEMES WHERE THEME_NAME='").append(name.toUpperCase());
		sb.append("' AND OWNER='").append(username.toUpperCase()).append("'");
		
		return getLayerMetadata(sb, stmt);
	}	
	private static LayerMetadata getLayerMetadata(StringBuffer querySb, Statement stmt) throws SQLException {
		LayerMetadata md = null;
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(querySb.toString());
			if (!rs.next()) {
				return null;
			}
			
			md = new LayerMetadata();
			md.mThemeTableSchema = rs.getString("OWNER");
			md.mThemeTableName = rs.getString("THEME_NAME");
			md.mThemeId = rs.getInt("THEME_ID");
			md.mViewLink = rs.getInt("VLINK");
			md.mBitEncodeValue = rs.getInt("BIT_ENCODE_VALUE");
			
			if (md.mViewLink > 0) {
				rs.close();
				querySb.setLength(0);
				
				querySb.append("SELECT MINX, MINY, MAXX, MAXY, GRID_SIZE ");
				querySb.append("FROM GSS.THEMES ");
				querySb.append("WHERE THEME_ID=").append(md.mViewLink);
				rs = stmt.executeQuery(querySb.toString());
				if (!rs.next()) {
					return null;
				}
			}
			
			md.mMinX = rs.getDouble("MINX");
			md.mMinY = rs.getDouble("MINY");
			md.mMaxX = rs.getDouble("MAXX");
			md.mMaxY = rs.getDouble("MAXY");
			md.mGridSize = rs.getDouble("GRID_SIZE");
			
			rs.close();
			
			querySb.setLength(0);
			querySb.append("SELECT B.F_GEOMETRY_COLUMN, B.G_TABLE_SCHEMA, B.G_TABLE_NAME,");
			querySb.append(" B.GEOMETRY_TYPE, B.STORAGE_TYPE, C.SRID, C.SRTEXT ");
			querySb.append("FROM GSS.GEOMETRY_COLUMNS B, GSS.SPATIAL_REF_SYS C ");
			querySb.append("WHERE B.F_TABLE_NAME='").append(md.mThemeTableName).append("'");
			querySb.append(" AND B.SRID=C.SRID");
			
			rs = stmt.executeQuery(querySb.toString());
			if (!rs.next()) {
				return null;
			}
			
			md.mGeometryColumn = rs.getString(1);
			md.mGeometryTableSchema = rs.getString(2);
			md.mGeometryTableName = rs.getString(3);
			md.mGeometryType = rs.getInt(4);
			md.mStorageType = rs.getInt(5);
			md.mSrId = rs.getInt(6);
			md.mCrs = rs.getString(7);

			
			rs.close();
			
			return md;
		}
		finally {
			if (rs != null && !rs.isClosed())
				rs.close();
		}
	}
	public static byte[] zipTileMatrixToBytes(final String jsonTileMatricies){
		
		Deflater def = new Deflater();

		int size = jsonTileMatricies.getBytes().length;
		def.setInput(jsonTileMatricies.getBytes());
		def.finish();
		
		byte[] zipBytes = new byte[size]; 
		
		def.deflate(zipBytes);
		def.end();

		return zipBytes;

	}	
	public static List<TileMatrix> unzipTileMatrixFromBytes(final byte[] jsonzipTileMatricies, int length){
		List<TileMatrix> items = new ArrayList<TileMatrix>();
		ObjectMapper objectMapper = new ObjectMapper();
		Inflater inf = new Inflater();
		inf.setInput(jsonzipTileMatricies);
		byte outputBuffer[] = new byte[length]; 
		try {

			inf.inflate(outputBuffer);
			String listOfObjects = new String(outputBuffer);

			inf.end();
			items = objectMapper.readValue(listOfObjects, objectMapper.getTypeFactory().constructParametricType(List.class, TileMatrix.class));
			
		} catch (DataFormatException | JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return items;
	}
	public static String getImageFormat(byte[] data) throws IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		Object source = bis;
		ImageInputStream iis = ImageIO.createImageInputStream(source);
		Iterator<?> readers = ImageIO.getImageReaders(iis);
		ImageReader reader = (ImageReader) readers.next();
		reader.setInput(iis, true);
		return reader.getFormatName();
	}

	public static BufferedImage getImage(byte[] data) throws IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		Object source = bis;
		ImageInputStream iis = ImageIO.createImageInputStream(source);
		Iterator<?> readers = ImageIO.getImageReaders(iis);
		ImageReader reader = (ImageReader) readers.next();
		reader.setInput(iis, true);
		ImageReadParam param = reader.getDefaultReadParam();
		return reader.read(0, param);
	}
}