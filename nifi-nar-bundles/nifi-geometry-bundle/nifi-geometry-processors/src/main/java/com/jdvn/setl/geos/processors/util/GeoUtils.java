package com.jdvn.setl.geos.processors.util;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GeoUtils {
	
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
			if (value.contains("MULTILINESTRING") || value.contains("LINESTRING") || value.contains("MULTIPOLYGON")
					|| value.contains("POLYGON") || value.contains("POINT") || value.contains("MULTIPOINT")
					|| value.contains("GEOMETRYCOLLECTION")) {
				
				geoKey = record.getSchema().getFields().get(i).getFieldName();
				break;
			}
		}
		return geoKey;

	}
    public static Map<String, Class<?>> createAttributeTableFromRecordSet(RecordReader avroReader, String geomFieldName){
        Map<String, Class<?>> attributes = new HashMap<>();
		try {
			List<RecordField> fields = avroReader.getSchema().getFields();
			for (int i = 0; i < fields.size(); i ++) {
				RecordField f = fields.get(i);
				DataType type = f.getDataType();
				Class<?> obj;
				switch (type.getFieldType()) {
				case LONG :
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
	public static SimpleFeatureCollection createSimpleFeatureCollectionFromNifiRecords(String collectionName, RecordReader avroReader, CoordinateReferenceSystem crs) {
        List<SimpleFeature> features = new ArrayList<>();
        String geomFieldName = "the_geom";
        String shpGeoColumn = "the_geom";
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
						case "MULTILINESTRING" :
							geometryClass = MultiLineString.class;
							break;
						case "LINESTRING" :
							geometryClass = LineString.class;
							break;					
						case "MULTIPOLYGON" :
							geometryClass = MultiLineString.class;
							break;
						case "POLYGON" :
							geometryClass = Polygon.class;
							break;	
						case "MULTIPOINT" :
							geometryClass = MultiPoint.class;
							break;
						case "POINT" :
							geometryClass = Point.class;
							break;				
						case "GEOMETRYCOLLECTION" :
							geometryClass = GeometryCollection.class;
							break;					
						default:
							geometryClass = MultiLineString.class;					
					}	
					
					Map<String, Class<?>> attributes = createAttributeTableFromRecordSet(avroReader, geomFieldName);
					// shp file with geo column is "the_geom"
					TYPE = generateFeatureType(collectionName, crs, shpGeoColumn, geometryClass, attributes);
			        featureBuilder = new SimpleFeatureBuilder(TYPE);
					bCreatedSchema = true;
				}
				GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		        WKTReader reader = new WKTReader( geometryFactory );
		        // Add geometry
		        Geometry geo = reader.read(record.getAsString(geomFieldName));
		        // Add attributes
				int size = record.getSchema().getFieldCount();
				Object[] objs = new Object[size];
		        for (int i = 0; i < size; i ++) {
		        	String fName = record.getSchema().getFieldNames().get(i);
		        	if ((fName == geomFieldName) && (geomFieldName != shpGeoColumn))
		        		fName = shpGeoColumn;
		        	int index = featureBuilder.getFeatureType().indexOf(fName);
		        	if (fName.contains(geomFieldName))
		        		objs[index]= geo;
		        	else
		        		objs[index] = record.getValue(fName);
		        	
		        }
		        featureBuilder.addAll(objs);
			    SimpleFeature feature = featureBuilder.buildFeature(null);
			    features.add(feature);
			}
			
			return new ListFeatureCollection(TYPE, features);
		} catch (IOException | MalformedRecordException | ParseException e) {
			logger.error("Could not create SimpleFeatureCollection because {}", new Object[]{e});
		} 
		return null;
	}

}