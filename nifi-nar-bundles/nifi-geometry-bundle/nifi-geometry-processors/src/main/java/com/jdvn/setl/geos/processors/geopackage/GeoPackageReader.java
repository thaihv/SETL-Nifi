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

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

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
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geopkg.GeoPackage;
import org.geotools.geopkg.GeoPkgDataStoreFactory;
import org.geotools.geopkg.TileEntry;
import org.geotools.geopkg.TileReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.jdvn.setl.geos.processors.util.GeoUtils;

@Tags({ "geopackage", "tiles", "raster", "vector","feature", "geospatial" })
@CapabilityDescription("Read data from a OGC geopackage file and encode geospatial data avro fomart with WKT for features.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class GeoPackageReader extends AbstractProcessor {

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

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		descriptors = new ArrayList<>();
		descriptors.add(FILENAME);
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
                final List<Record> records = getRecordsFromFeatureTable(store,name);
                
                if (records.isEmpty() == false) {
                	
                    final long importStart = System.nanoTime();
                    
                    FlowFile transformed = session.create(flowFile);
    				CoordinateReferenceSystem myCrs = getCRSFromFeatureTable(store,name);
                    RecordSchema recordSchema = records.get(0).getSchema();                
                    transformed = session.write(transformed, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                			final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                			@SuppressWarnings("resource")  
                			final RecordSetWriter writer = new WriteAvroResultWithSchema(avroSchema, out, CodecFactory.nullCodec());            				
                			writer.write(new ListRecordSet(recordSchema, records));
                        }
                    });                

                    
                    final long importNanos = System.nanoTime() - importStart;
                    final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
                    
                    session.getProvenanceReporter().receive(transformed, file.toURI().toString(), importMillis);
                    transformed = session.putAttribute(transformed, GeoAttributes.CRS.key(), myCrs.toWKT());
                    transformed = session.putAttribute(transformed, GeoAttributes.GEO_TYPE.key(), "Features");
                    transformed = session.putAttribute(transformed, GeoAttributes.GEO_NAME.key(), name);
                    transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/avro+geowkt");
                    session.transfer(transformed, REL_SUCCESS);   

                    logger.info("Features added {} to flow", new Object[]{transformed});
                } 
			}
			store.dispose();
			
			// Process Tiles tables
			GeoPackage geoPackage = new GeoPackage(file);
			for (int i = 0; i < geoPackage.tiles().size(); i++) {
				TileEntry t = geoPackage.tiles().get(i);
				final List<Record> records = GeoUtils.getTilesRecordFromTileEntry(geoPackage, t);
				if (records.isEmpty() == false) {
	                final long importStart = System.nanoTime();
	                // Create flowfile
	                FlowFile transformed = session.create(flowFile);
	                
	                //Get geo attributes
	                CoordinateReferenceSystem myCrs = getCRSFromTilesTable(file,geoPackage.tiles().get(i));
	                TileReader r = geoPackage.reader(t, null, null, null, null, null, null);
	                String imgType = getImageFormat(r.next().getData());

                    RecordSchema recordSchema = records.get(0).getSchema();                
                    transformed = session.write(transformed, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                			final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                			@SuppressWarnings("resource")  
                			final RecordSetWriter writer = new WriteAvroResultWithSchema(avroSchema, out, CodecFactory.nullCodec());            				
                			writer.write(new ListRecordSet(recordSchema, records));
                        }
                    });                
					
	                final long importNanos = System.nanoTime() - importStart;
	                final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
	                
	                session.getProvenanceReporter().receive(transformed, file.toURI().toString(), importMillis);
	                transformed = session.putAttribute(transformed, GeoAttributes.CRS.key(), myCrs.toWKT());
	                transformed = session.putAttribute(transformed, GeoAttributes.GEO_TYPE.key(), "Tiles");
	                transformed = session.putAttribute(transformed, GeoAttributes.GEO_NAME.key(), t.getTableName());
	                transformed = session.putAttribute(transformed, GeoAttributes.GEO_RASTER_TYPE.key(), imgType);
	                transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/avro+binary");
	                session.transfer(transformed, REL_SUCCESS);   

	                logger.info("Tiles added {} to flow", new Object[]{transformed});
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
	protected BufferedImage getImage(byte[] data) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        Object source = bis; 
        ImageInputStream iis = ImageIO.createImageInputStream(source); 
        Iterator<?> readers = ImageIO.getImageReaders(iis);
        ImageReader reader = (ImageReader) readers.next();
        reader.setInput(iis, true);
        ImageReadParam param = reader.getDefaultReadParam();
        return reader.read(0, param);
    }	
	public String getImageFormat(byte[] data) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        Object source = bis; 
        ImageInputStream iis = ImageIO.createImageInputStream(source); 
        Iterator<?> readers = ImageIO.getImageReaders(iis);
        ImageReader reader = (ImageReader) readers.next();
        reader.setInput(iis, true);
        return reader.getFormatName();
    }	
	public ArrayList<Record> getRecordsFromFeatureTable(DataStore store, String tableName) {
		final ArrayList<Record> returnRs = new ArrayList<Record>();
		try {
			SimpleFeatureSource featureSource = store.getFeatureSource(tableName);
			SimpleFeatureType schema = featureSource.getSchema();
			final List<RecordField> fields = new ArrayList<>();
			for (int i = 0; i < schema.getAttributeCount(); i++) {
				String fieldName = schema.getDescriptor(i).getName().getLocalPart();
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
			SimpleFeatureCollection features = featureSource.getFeatures();
			SimpleFeatureIterator it = (SimpleFeatureIterator) features.features();
			final RecordSchema recordSchema = new SimpleRecordSchema(fields);
			while (it.hasNext()) {
				SimpleFeature feature = it.next();
				Map<String, Object> fieldMap = new HashMap<String, Object>();
				for (int i = 0; i < feature.getAttributeCount(); i++) {
					String key = feature.getFeatureType().getDescriptor(i).getName().getLocalPart();
					Object value = feature.getAttribute(i);
					fieldMap.put(key, value);
				}
				Record r = new MapRecord(recordSchema, fieldMap);
				returnRs.add(r);
				//System.out.println(r);
			}
			it.close();
			return returnRs;

		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}
		return returnRs;
	}
	public CoordinateReferenceSystem getCRSFromFeatureTable(DataStore store, String tableName) {
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
	public CoordinateReferenceSystem getCRSFromTilesTable(final File geopkg, TileEntry tileEntry) {
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
}
