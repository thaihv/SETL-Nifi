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
package com.jdvn.setl.geos.processors.geotransform;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroReaderWithEmbeddedSchema;
import org.apache.nifi.avro.AvroRecordReader;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.avro.WriteAvroResultWithSchema;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.GeoAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StopWatch;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.jdvn.setl.geos.processors.util.GeoUtils;

@Tags({ "Features", "Transformation", "Buffer", "Simplify", "Attributes", "Geospatial" })
@CapabilityDescription("Transform features from a given flow file to other Geometry Type.")
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })

public class Geotransform extends AbstractProcessor {

    public static final AllowableValue Simplify = new AllowableValue("Simplify", "Simplify", "Simplify all features in dataflow");
    public static final AllowableValue Buffer = new AllowableValue("Buffer", "Buffer", "Buffering for each features in dataflow");
    public static final AllowableValue Centroid = new AllowableValue("Centroid", "Centroid", "Calculate centroid for each features in dataflow");
    
    public static final AllowableValue CAP_ROUND = new AllowableValue("1", "CAP_ROUND", "The ends of linestrings as round");
    public static final AllowableValue CAP_FLAT = new AllowableValue("2", "CAP_FLAT", "The ends of linestrings as flat");
    public static final AllowableValue CAP_SQUARE = new AllowableValue("3", "CAP_SQUARE", "The ends of linestrings as square");
    
    public static final PropertyDescriptor TRANS_TYPE = new PropertyDescriptor.Builder()
            .name("type-geo-tranform")
            .displayName("Tranformation Type")
            .description("Which type of transformation to execute for incomping datafile. It could be Simplify, Buffering or Centroid")
            .required(true)
            .allowableValues(Simplify, Buffer, Centroid)
            .defaultValue(Simplify.getValue())
            .build();
	public static final PropertyDescriptor DISTANCE = new PropertyDescriptor.Builder()
			.name("Distance Tolerance")
			.displayName("distance")
			.description("The tolerance to use for a simplified version of the geometry")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.dependsOn(TRANS_TYPE, Simplify.getValue(), Buffer.getValue() )
			.build();
	public static final PropertyDescriptor QSEGMENTS = new PropertyDescriptor.Builder()
			.name("QuadrantSegments")
			.displayName("quadrantSegments")
			.description("The number of line segments used to represent a quadrant of a circle.")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.dependsOn(TRANS_TYPE, Buffer.getValue() )
			.build();
	public static final PropertyDescriptor ENDCAPSTYLE = new PropertyDescriptor.Builder()
			.name("EndCapStyle")
			.displayName("endCapStyle")
			.description("The end cap style to use for be created at the ends of linestrings Values as: CAP_ROUND (default = 1);CAP_FLAT = 2; CAP_SQUARE = 3")
			.required(true)
			.allowableValues(CAP_ROUND, CAP_FLAT, CAP_SQUARE)
            .defaultValue(CAP_ROUND.getValue())
			.dependsOn(TRANS_TYPE, Buffer.getValue() )
			.build();	
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("Flowfiles that have been successfully transformed are transferred to this relationship")
			.build();
	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("Flowfiles that could not be transformed for some reason are transferred to this relationship")
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
		supDescriptors.add(TRANS_TYPE);
		supDescriptors.add(DISTANCE);
		supDescriptors.add(QSEGMENTS);
		supDescriptors.add(ENDCAPSTYLE);
		properties = Collections.unmodifiableList(supDescriptors);
	}

	@OnScheduled
	public void setup(final ProcessContext context) {

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
		final StopWatch stopWatch = new StopWatch(true);
		final ComponentLog logger = getLogger();
		try {

			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(final InputStream in) {
					try {
						AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(in);
						final String srs_source = flowFile.getAttributes().get(GeoAttributes.CRS.key());
						final CoordinateReferenceSystem crs_source = CRS.parseWKT(srs_source);
						String transfrom_type = context.getProperty(TRANS_TYPE).evaluateAttributeExpressions(flowFile).getValue();
						
						Map<String, Object> map = new TreeMap<>();				
						for (int i = 0; i < properties.size(); i++) {
							PropertyDescriptor p = properties.get(i);
							if (p.getName().equals("type-geo-tranform")) 
								continue;
							String value = context.getProperty(p).evaluateAttributeExpressions(flowFile).getValue();
							map.put(p.getDisplayName(), value);
						}
						
						
						SimpleFeatureCollection collection = GeoUtils.createSimpleFeatureCollectionWithGeoTransform("featurecollection", reader, crs_source, transfrom_type, map);												
						final RecordSchema recordSchema = GeoUtils.createFeatureRecordSchema(collection);
						List<Record> records = GeoUtils.getNifiRecordsFromFeatureCollection(collection);

						FlowFile transformed = session.create(flowFile);
						transformed = session.write(transformed, new OutputStreamCallback() {
							@Override
							public void process(final OutputStream out) throws IOException {
								final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
								@SuppressWarnings("resource")
								final RecordSetWriter writer = new WriteAvroResultWithSchema(avroSchema, out, CodecFactory.bzip2Codec());
								writer.write(new ListRecordSet(recordSchema, records));
								writer.flush();
							}
						});
						session.getProvenanceReporter().receive(transformed, flowFile.getAttributes().get(GeoUtils.GEO_URL), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
						session.transfer(transformed, REL_SUCCESS);
						session.adjustCounter("Records Written", records.size(), false);
						
					} catch (IOException | FactoryException e) {
						logger.error("Could not transformed {} because {}", new Object[] { flowFile, e });
						session.transfer(flowFile, REL_FAILURE);
						return;
					}
				}
			});
			session.remove(flowFile);

		} catch (Exception e) {
			logger.error("Could not transformed {} because {}", new Object[] { flowFile, e });
			session.transfer(flowFile, REL_FAILURE);
			return;
		}

	}

}
