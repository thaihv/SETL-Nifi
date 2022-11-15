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
package com.jdvn.setl.geos.processors.geopackage;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.avro.WriteAvroResultWithSchema;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.flowfile.attributes.GeoAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StopWatch;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.geopkg.GeoPackage;
import org.geotools.geopkg.GeoPkgDataStoreFactory;
import org.geotools.geopkg.TileEntry;
import org.geotools.geopkg.TileMatrix;
import org.geotools.geopkg.TileReader;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jdvn.setl.geos.processors.util.GeoUtils;

@Tags({ "geopackage", "tiles", "raster", "vector","feature", "geospatial" })
@CapabilityDescription("Read data from a OGC geopackage file and encode geospatial data avro fomart with WKT for features.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class GeoPackageReader extends AbstractProcessor {

    public static final String RESULT_TABLENAME = "source.tablename";
    public static final String RESULT_SCHEMANAME = "source.schemaname";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key(); 
	static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder().name("Geopackage File to Fetch")
			.description("The fully-qualified filename of the file to fetch from the file system")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.defaultValue("${absolute.path}/${filename}").required(true).build();

	static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("Geopackage data is routed to success").build();
	static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description(
			"Any FlowFile that could not be fetched from the file system for any reason will be transferred to this Relationship.")
			.build();
    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("shapefile-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "shape file into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build(); 
	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		descriptors = new ArrayList<>();
		descriptors.add(FILENAME);
		descriptors.add(MAX_ROWS_PER_FLOW_FILE);
		descriptors = Collections.unmodifiableList(descriptors);

		relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) {
		final String filename = context.getProperty(FILENAME).evaluateAttributeExpressions().getValue();
		final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
		final File file = new File(filename);
		final Path filePath = file.toPath();
		final ComponentLog logger = getLogger();
		
		FlowFile flowFile = null;
		
        flowFile = session.create();
        flowFile = session.importFrom(filePath, true, flowFile);

		HashMap<String, Object> map = new HashMap<>();
		map.put(GeoPkgDataStoreFactory.DBTYPE.key, "geopkg");
		map.put(GeoPkgDataStoreFactory.DATABASE.key, filename);
		
		try {
			// Process feature tables
			DataStore store = DataStoreFinder.getDataStore(map);
			String[] names = store.getTypeNames();
			for (String name : names) {
				SimpleFeatureSource featureSource = store.getFeatureSource(name);
				
				SimpleFeatureCollection	selectedfeatures = featureSource.getFeatures();
				String geofieldName = selectedfeatures.getSchema().getGeometryDescriptor().getLocalName();
				
				int maxRecord = featureSource.getFeatures().size();
				if (maxRecord <= 0) {
					return;
				}
				final RecordSchema recordSchema = GeoUtils.createFeatureRecordSchema(featureSource);
				if (maxRowsPerFlowFile > 0 && maxRowsPerFlowFile < maxRecord) {
					final String fragmentIdentifier = UUID.randomUUID().toString();
					int fragmentIndex = 0;
					int from = 1;
					int to = 0;
					while (from < maxRecord) {
						final StopWatch stopWatch = new StopWatch(true);
						to = from + maxRowsPerFlowFile;
						if (to > maxRecord)
							to = maxRecord;
		                final List<Record> records = GeoUtils.getNifiRecordSegmentsFromGeoPackageFeatureTable(file,name,geofieldName,recordSchema,from,to);
		                if (records.isEmpty() == false) {
		                    FlowFile transformed = session.create(flowFile);
		    				CoordinateReferenceSystem myCrs = GeoUtils.getCRSFromGeoPackageFeatureTable(store,name);             
		                    transformed = session.write(transformed, new OutputStreamCallback() {
		                        @Override
		                        public void process(final OutputStream out) throws IOException {
		                			final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
		                			@SuppressWarnings("resource")  
		                			final RecordSetWriter writer = new WriteAvroResultWithSchema(avroSchema, out, CodecFactory.nullCodec());            				
		                			writer.write(new ListRecordSet(recordSchema, records));
		                			writer.flush();
		                        }
		                    });                
		                    transformed = session.putAttribute(transformed, CoreAttributes.FILENAME.key(), name);
		                    transformed = session.putAttribute(transformed, GeoAttributes.CRS.key(), myCrs.toWKT());
		                    transformed = session.putAttribute(transformed, GeoAttributes.GEO_TYPE.key(), "Features");
							transformed = session.putAttribute(transformed, GeoAttributes.GEO_NAME.key(), name + ":" + fragmentIdentifier + ":" + String.valueOf(fragmentIndex));
		                    transformed = session.putAttribute(transformed, GeoUtils.GEO_URL, file.toURI().toString());
		                    transformed = session.putAttribute(transformed, RESULT_TABLENAME, name);
		                    transformed = session.putAttribute(transformed, GeoAttributes.GEO_RECORD_NUM.key(), String.valueOf(records.size()));
							transformed = session.putAttribute(transformed, FRAGMENT_ID, fragmentIdentifier);
							transformed = session.putAttribute(transformed, FRAGMENT_INDEX, String.valueOf(fragmentIndex));
		                    transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/avro+geowkt");

		                    session.getProvenanceReporter().receive(transformed, file.toURI().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
		                    logger.info("Features added {} to flow", new Object[]{transformed});
		                    session.adjustCounter("Records Read", records.size(), false);
		                    session.transfer(transformed, REL_SUCCESS); 
		                }
						from = to;
						fragmentIndex++;
					}
				}
				else {
					final StopWatch stopWatch = new StopWatch(true);
	                final List<Record> records = GeoUtils.getNifiRecordsFromGeoPackageFeatureTable(featureSource,name,recordSchema);
	                if (records.isEmpty() == false) {
	                    FlowFile transformed = session.create(flowFile);
	    				CoordinateReferenceSystem myCrs = GeoUtils.getCRSFromGeoPackageFeatureTable(store,name);             
	                    transformed = session.write(transformed, new OutputStreamCallback() {
	                        @Override
	                        public void process(final OutputStream out) throws IOException {
	                			final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
	                			@SuppressWarnings("resource")  
	                			final RecordSetWriter writer = new WriteAvroResultWithSchema(avroSchema, out, CodecFactory.nullCodec());            				
	                			writer.write(new ListRecordSet(recordSchema, records));
	                			writer.flush();
	                        }
	                    });
	                    transformed = session.putAttribute(transformed, CoreAttributes.FILENAME.key(), name);
	                    transformed = session.putAttribute(transformed, GeoAttributes.CRS.key(), myCrs.toWKT());
	                    transformed = session.putAttribute(transformed, GeoAttributes.GEO_TYPE.key(), "Features");
	                    transformed = session.putAttribute(transformed, GeoAttributes.GEO_NAME.key(), name);
	                    transformed = session.putAttribute(transformed, GeoUtils.GEO_URL, file.toURI().toString());
	                    transformed = session.putAttribute(transformed, RESULT_TABLENAME, name);

	                    transformed = session.putAttribute(transformed, GeoAttributes.GEO_RECORD_NUM.key(), String.valueOf(records.size()));
	                    transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/avro+geowkt");
	                      
	                    session.getProvenanceReporter().receive(transformed, file.toURI().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
	                    logger.info("Features added {} to flow", new Object[]{transformed});
	                    session.adjustCounter("Records Read", records.size(), false);
	                    session.transfer(transformed, REL_SUCCESS); 
	                }					
				}
 
			}
			store.dispose();
			// Process Tiles tables
			GeoPackage geoPackage = new GeoPackage(file);
			for (int i = 0; i < geoPackage.tiles().size(); i++) {
				TileEntry t = geoPackage.tiles().get(i);
				ReferencedEnvelope envelope = t.getBounds();
				final List<TileMatrix> tileMatricies = t.getTileMatricies();
				
				ObjectMapper objectMapper = new ObjectMapper();
				String jsonTileMatricies = objectMapper.writeValueAsString(tileMatricies);
				int tileMatrixBuffLen = jsonTileMatricies.getBytes().length;
				byte[] compTileMatrix = GeoUtils.zipTileMatrixToBytes(jsonTileMatricies);
				byte[] encoded = Base64.getEncoder().encode(compTileMatrix);

                //Get geo attributes
                CoordinateReferenceSystem myCrs = GeoUtils.getCRSFromGeoPackageTilesTable(file,geoPackage.tiles().get(i));
                TileReader r = geoPackage.reader(t, null, null, null, null, null, null);
                String imgType = GeoUtils.getImageFormat(r.next().getData());
                int minMax[] = GeoUtils.getMinMaxTilesZoomTileEntry(geoPackage, t);
                String szEnvelop = envelope.toString().substring(envelope.toString().indexOf("["));
                String center  = "[" + envelope.centre().x + ", " + envelope.centre().y + "]";
                
                final RecordSchema recordSchema = GeoUtils.createTileRecordSchema(t);
                
				int maxRecord = GeoUtils.getTileRecordCount(geoPackage, t);
				if (maxRecord <= 0) {
					return;
				}
				if (maxRowsPerFlowFile > 0 && maxRowsPerFlowFile < maxRecord) {
					final String fragmentIdentifier = UUID.randomUUID().toString();
					int fragmentIndex = 0;
					int from = 1;
					int to = 0;
					while (from < maxRecord) {
						final StopWatch stopWatch = new StopWatch(true);
						to = from + maxRowsPerFlowFile;
						if (to > maxRecord)
							to = maxRecord;
		                final List<Record> records = GeoUtils.getNifiRecordSegmentsFromTileEntry(geoPackage,t,from,to);
		                if (records.isEmpty() == false) {
			                // Create flowfile
			                FlowFile transformed = session.create(flowFile);			                           
		                    transformed = session.write(transformed, new OutputStreamCallback() {
		                        @Override
		                        public void process(final OutputStream out) throws IOException {
		                			final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
		                			@SuppressWarnings("resource")  
		                			final RecordSetWriter writer = new WriteAvroResultWithSchema(avroSchema, out, CodecFactory.nullCodec());            				
		                			writer.write(new ListRecordSet(recordSchema, records));
		                			writer.flush();
		                        }
		                    });                
							
		                    transformed = session.putAttribute(transformed, CoreAttributes.FILENAME.key(), t.getTableName());
			                transformed = session.putAttribute(transformed, GeoAttributes.CRS.key(), myCrs.toWKT());
			                transformed = session.putAttribute(transformed, GeoAttributes.GEO_TYPE.key(), "Tiles");             
			                transformed = session.putAttribute(transformed, GeoAttributes.GEO_ENVELOPE.key(), szEnvelop);
			                transformed = session.putAttribute(transformed, GeoAttributes.GEO_CENTER.key(), center);
			                transformed = session.putAttribute(transformed, GeoAttributes.GEO_TILE_MATRIX.key(), new String(encoded));
			                transformed = session.putAttribute(transformed, GeoUtils.GEO_TTLE_MATRIX_BYTES_LEN, String.valueOf(tileMatrixBuffLen));
			                transformed = session.putAttribute(transformed, GeoAttributes.GEO_RASTER_TYPE.key(), imgType);
			                transformed = session.putAttribute(transformed, GeoAttributes.GEO_RECORD_NUM.key(), String.valueOf(records.size()));
			                transformed = session.putAttribute(transformed, GeoUtils.GEO_URL, file.toURI().toString());
			                transformed = session.putAttribute(transformed, RESULT_TABLENAME, t.getTableName());
			                transformed = session.putAttribute(transformed, GeoAttributes.GEO_ZOOM_MIN.key(), String.valueOf(minMax[0]));
			                transformed = session.putAttribute(transformed, GeoAttributes.GEO_ZOOM_MAX.key(), String.valueOf(minMax[1]));			                
			                transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/avro+geotiles");
							transformed = session.putAttribute(transformed, FRAGMENT_ID, fragmentIdentifier);
							transformed = session.putAttribute(transformed, FRAGMENT_INDEX, String.valueOf(fragmentIndex));
			                transformed = session.putAttribute(transformed, GeoAttributes.GEO_NAME.key(), t.getTableName() + ":" + fragmentIdentifier + ":" + String.valueOf(fragmentIndex));
			         
			                session.getProvenanceReporter().receive(transformed, file.toURI().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
			                logger.info("Tiles added {} to flow", new Object[]{transformed});
			                session.adjustCounter("Records Read", records.size(), false);
			                session.transfer(transformed, REL_SUCCESS); 
		                }
						from = to;
						fragmentIndex++;
					}
				}
				else {
					final StopWatch stopWatch = new StopWatch(true);
					final List<Record> records = GeoUtils.getNifiRecordsFromTileEntry(geoPackage, t);
					if (records.isEmpty() == false) {

		                // Create flowfile
		                FlowFile transformed = session.create(flowFile);		                               
	                    transformed = session.write(transformed, new OutputStreamCallback() {
	                        @Override
	                        public void process(final OutputStream out) throws IOException {
	                			final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
	                			@SuppressWarnings("resource")  
	                			final RecordSetWriter writer = new WriteAvroResultWithSchema(avroSchema, out, CodecFactory.nullCodec());            				
	                			writer.write(new ListRecordSet(recordSchema, records));
	                			writer.flush();
	                        }
	                    });                
						
	                    transformed = session.putAttribute(transformed, CoreAttributes.FILENAME.key(), t.getTableName());
		                transformed = session.putAttribute(transformed, GeoAttributes.CRS.key(), myCrs.toWKT());
		                transformed = session.putAttribute(transformed, GeoAttributes.GEO_TYPE.key(), "Tiles");
		                transformed = session.putAttribute(transformed, GeoAttributes.GEO_ENVELOPE.key(), szEnvelop);
		                transformed = session.putAttribute(transformed, GeoAttributes.GEO_CENTER.key(), center);
		                transformed = session.putAttribute(transformed, GeoAttributes.GEO_TILE_MATRIX.key(), new String(encoded));
		                transformed = session.putAttribute(transformed, GeoUtils.GEO_TTLE_MATRIX_BYTES_LEN, String.valueOf(tileMatrixBuffLen));
		                transformed = session.putAttribute(transformed, GeoAttributes.GEO_RASTER_TYPE.key(), imgType);
		                transformed = session.putAttribute(transformed, GeoAttributes.GEO_RECORD_NUM.key(), String.valueOf(records.size()));
		                transformed = session.putAttribute(transformed, GeoUtils.GEO_URL, file.toURI().toString());
		                transformed = session.putAttribute(transformed, RESULT_TABLENAME, t.getTableName());		                		                
		                transformed = session.putAttribute(transformed, GeoAttributes.GEO_ZOOM_MIN.key(), String.valueOf(minMax[0]));
		                transformed = session.putAttribute(transformed, GeoAttributes.GEO_ZOOM_MAX.key(), String.valueOf(minMax[1]));		                
		                transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/avro+geotiles");
		                transformed = session.putAttribute(transformed, GeoAttributes.GEO_NAME.key(), t.getTableName());
		                  
		                session.getProvenanceReporter().receive(transformed, file.toURI().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
		                logger.info("Tiles added {} to flow", new Object[]{transformed});
		                session.adjustCounter("Records Read", records.size(), false);
		                session.transfer(transformed, REL_SUCCESS); 
					}					
				}

			}
			geoPackage.close();
			
		} catch (IOException e) {
			
            logger.error("Failed to retrieve files due to {}", e);
            // anything that we've not already processed needs to be put back on the queue
            if (flowFile != null) {
                session.remove(flowFile);
            }
		}
		session.remove(flowFile);
	}

}
