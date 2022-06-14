/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jdvn.setl.geos.processors.shapefile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroReaderWithEmbeddedSchema;
import org.apache.nifi.avro.AvroRecordReader;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.GeoAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.CRS;
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
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;


@Tags({ "shape file", "wkt", "json", "geospatial" })
@CapabilityDescription("Write geospatial data into a given shape file.")
@SeeAlso({ ShpReader.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class ShpWriter extends AbstractProcessor {

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";

    public static final String FILE_MODIFY_DATE_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static final Pattern RWX_PATTERN = Pattern.compile("^([r-][w-])([x-])([r-][w-])([x-])([r-][w-])([x-])$");
    public static final Pattern NUM_PATTERN = Pattern.compile("^[0-7]{3}$");

    private static final Validator PERMISSIONS_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            ValidationResult.Builder vr = new ValidationResult.Builder();
            if (context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            if (RWX_PATTERN.matcher(input).matches() || NUM_PATTERN.matcher(input).matches()) {
                return vr.valid(true).build();
            }
            return vr.valid(false)
                    .subject(subject)
                    .input(input)
                    .explanation("This must be expressed in rwxr-x--- form or octal triplet form.")
                    .build();
        }
    };
    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The directory to which files should be written. You may use expression language such as /aa/bb/${path}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MAX_DESTINATION_FILES = new PropertyDescriptor.Builder()
            .name("Maximum File Count")
            .description("Specifies the maximum number of files that can exist in the output directory")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("CharSet")
            .description("The name of charset for attributes in target shapfiles")
            .required(true)
            .defaultValue("ISO-8859-1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION)
            .build();
    public static final PropertyDescriptor CHANGE_LAST_MODIFIED_TIME = new PropertyDescriptor.Builder()
            .name("Last Modified Time")
            .description("Sets the lastModifiedTime on the output file to the value of this attribute.  Format must be yyyy-MM-dd'T'HH:mm:ssZ.  "
                    + "You may also use expression language such as ${file.lastModifiedTime}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_PERMISSIONS = new PropertyDescriptor.Builder()
            .name("Permissions")
            .description("Sets the permissions on the output file to the value of this attribute.  Format must be either UNIX rwxrwxrwx with a - in "
                    + "place of denied permissions (e.g. rw-r--r--) or an octal number (e.g. 644).  You may also use expression language such as "
                    + "${file.permissions}.")
            .required(false)
            .addValidator(PERMISSIONS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .description("Sets the owner on the output file to the value of this attribute.  You may also use expression language such as "
                    + "${file.owner}. Note on many operating systems Nifi must be running as a super-user to have the permissions to set the file owner.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_GROUP = new PropertyDescriptor.Builder()
            .name("Group")
            .description("Sets the group on the output file to the value of this attribute.  You may also use expression language such "
                    + "as ${file.group}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .description("If true, then missing destination directories will be created. If false, flowfiles are penalized and sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final int MAX_FILE_LOCK_ATTEMPTS = 10;
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output directory are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        // relationships
        final Set<Relationship> procRels = new HashSet<>();
        procRels.add(REL_SUCCESS);
        procRels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(procRels);

        // descriptors
        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
        supDescriptors.add(DIRECTORY);
        supDescriptors.add(CONFLICT_RESOLUTION);
        supDescriptors.add(CHARSET);
        supDescriptors.add(CREATE_DIRS);
        supDescriptors.add(MAX_DESTINATION_FILES);
        supDescriptors.add(CHANGE_LAST_MODIFIED_TIME);
        supDescriptors.add(CHANGE_PERMISSIONS);
        supDescriptors.add(CHANGE_OWNER);
        supDescriptors.add(CHANGE_GROUP);
        properties = Collections.unmodifiableList(supDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
        final long importStart = System.nanoTime();
        final ComponentLog logger = getLogger();
        
        String filename = flowFile.getAttributes().get(CoreAttributes.FILENAME.key());
        if (FilenameUtils.getExtension(filename).contains("shp") == false)
        	filename = filename + ".shp";
        
		File srcFile = new File(context.getProperty(DIRECTORY) + "/" + filename);
		String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue();
		try {
			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(final InputStream in) {
					try {
			
						AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(in);
						final String srs = flowFile.getAttributes().get(GeoAttributes.CRS.key());
						SimpleFeatureCollection collection = createSimpleFeatureCollectionFromNifiRecords(reader, CRS.parseWKT(srs));
						createShapeFileFromGeoDataFlowfile(srcFile, charset, collection);
		                logger.info("saved {} to file {}", new Object[]{flowFile, srcFile.toURI().toString()});
												
					} catch (IOException | FactoryException e) {
						logger.error("Could not save {} because {}", new Object[]{flowFile, e});
						session.transfer(flowFile, REL_FAILURE);
						return;
					}
				}

			});

		} catch (Exception e) {
			logger.error("Could not save {} because {}", new Object[]{flowFile, e});
			session.transfer(flowFile, REL_FAILURE);
		}
		
        final long importNanos = System.nanoTime() - importStart;
        final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
		session.getProvenanceReporter().receive(flowFile, srcFile.toURI().toString(), importMillis);
		session.transfer(flowFile, REL_SUCCESS);
	}
	public void createShapeFileFromGeoDataFlowfile(File srcFile, String charsetName, SimpleFeatureCollection collection) {
		final ComponentLog logger = getLogger();
		SimpleFeatureType schema = null;
		if (collection.features().hasNext())
			schema = collection.features().next().getFeatureType();
		
		ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
		Map<String, Serializable> params = new HashMap<>();
		Transaction transaction = null;
		try {
			params.put("url", srcFile.toURI().toURL());
			params.put("create spatial index", Boolean.TRUE);
			ShapefileDataStore newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
			newDataStore.setCharset(Charset.forName(charsetName));
			newDataStore.createSchema(schema);
			/*
			 * Write the features to the shapefile
			 */
			transaction = new DefaultTransaction("create");

			String typeName = newDataStore.getTypeNames()[0];
			SimpleFeatureSource featureTarget = newDataStore.getFeatureSource(typeName);

			if (featureTarget instanceof SimpleFeatureStore) {
				SimpleFeatureStore featureStore = (SimpleFeatureStore) featureTarget;

				featureStore.setTransaction(transaction);
				try {
					featureStore.addFeatures(collection);
					transaction.commit();
				} catch (Exception problem) {
					problem.printStackTrace();
					transaction.rollback();
				} finally {
					transaction.close();
				}
			} else {
				logger.error(typeName + " does not support read/write access");
			}

		} catch (IOException e) {
			logger.error("Could not create Shape File because {}", new Object[]{e});
		} 
	} 
	
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
	public Map<String, Class<?>> createAttributeTableFromRecordSet(RecordReader avroReader, String geomFieldName){
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

	public String getGeometryFieldName(Record record) {
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public SimpleFeatureCollection createSimpleFeatureCollectionFromNifiRecords(RecordReader avroReader, CoordinateReferenceSystem crs) {
		final ComponentLog logger = getLogger();
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
					TYPE = generateFeatureType("shpfile", crs, shpGeoColumn, geometryClass, attributes);
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
