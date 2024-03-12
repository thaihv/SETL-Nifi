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
import java.util.Set;
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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
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
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.jdvn.setl.geos.processors.util.GeoUtils;

@Tags({ "Coordinate Reference System", "WKT", "EPSG", "ESRI", "Attributes", "Geospatial" })
@CapabilityDescription("Transform data from a given flow file to other CRS.")
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })

public class CRStransform extends AbstractProcessor {
	public static final String GEO_COLUMN = "geo.column";
	public static final PropertyDescriptor USER_DEFINED = new PropertyDescriptor.Builder()
			.name("Use User-Defined CRS")
			.description("If true, then use CRS in format WKT for target flowfile.")
			.required(true)
			.allowableValues("true", "false")
			.defaultValue("false")
			.build();
	public static final PropertyDescriptor CRS_TARGET_WKT = new PropertyDescriptor.Builder()
			.name("CRS in OGC WKT format")
			.description("The presentation of Coordinate Reference System in OGC Well-Known Text Format . "
					+ "If the number is 0, the Coordinate Reference System will be set same as CRS flowfile attribute")
			.required(true)
			.defaultValue("0")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.dependsOn(USER_DEFINED, "true")
			.build();
	public static final PropertyDescriptor LENIENT = new PropertyDescriptor.Builder()
			.name("Lenient Datum Shift")
			.description("It can optionally tolerate lenient datum shift. If the lenient argument is true, then this method will not throw a "
					+ "Bursa-Wolf parameters required exception during datum shifts if the Bursa-Wolf paramaters are not specified")
			.required(true)
			.defaultValue("false")
			.allowableValues("true", "false")
			.build();
	private static AllowableValue[] capabilitiesCrsIdentifiers;
	private static String crsDefault;

	public static PropertyDescriptor CRS_TARGET;
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

		List<AllowableValue> listCrsIdentifiers = new ArrayList<AllowableValue>();

		for (String code : CRS.getSupportedCodes("EPSG")) {
			if ("WGS84(DD)".equals(code))
				continue;
			if (code.contains("EPSG"))
				listCrsIdentifiers.add(new AllowableValue(code));
		}
		for (String code : CRS.getSupportedCodes("ESRI")) {
			if ("WGS84(DD)".equals(code))
				continue;
			listCrsIdentifiers.add(new AllowableValue("ESRI:" + code));
		}

		capabilitiesCrsIdentifiers = new AllowableValue[listCrsIdentifiers.size()];
		listCrsIdentifiers.toArray(capabilitiesCrsIdentifiers);
		crsDefault = "EPSG:4326";

		// relationships
		final Set<Relationship> procRels = new HashSet<>();
		procRels.add(REL_SUCCESS);
		procRels.add(REL_FAILURE);
		relationships = Collections.unmodifiableSet(procRels);

		// descriptors
		final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
		supDescriptors.add(USER_DEFINED);

		CRS_TARGET = new PropertyDescriptor.Builder()
				.name("Target CRS").description("The presentation of Coordinate Reference System of spatial dataflow output in OGC Well-Known Text Format")
				.required(true)
				.allowableValues(capabilitiesCrsIdentifiers)
				.defaultValue(crsDefault)
				.dependsOn(USER_DEFINED, "false")
				.build();
		supDescriptors.add(CRS_TARGET);
		supDescriptors.add(CRS_TARGET_WKT);
		supDescriptors.add(LENIENT);		
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

						final CoordinateReferenceSystem crs_target;

						final boolean bUserCRS = context.getProperty(USER_DEFINED).asBoolean();
						final boolean bLenient = context.getProperty(LENIENT).asBoolean();
						final String srs_target;
						if (bUserCRS) {
							srs_target = context.getProperty(CRS_TARGET_WKT).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[\\r\\n\\t ]", "");
							crs_target = CRS.parseWKT(srs_target);
						} else {  // Use code to generate CRS
							srs_target = context.getProperty(CRS_TARGET).evaluateAttributeExpressions(flowFile).getValue();
							crs_target = CRS.decode(srs_target);
						}

						SimpleFeatureCollection collection = GeoUtils.createSimpleFeatureCollectionWithCRSTransformed("crs_transformed", reader, crs_source, crs_target, bLenient);
						
						// Center and envelope for all features
						String center = null;
						String envelope = null;								
						int maxRecord = collection.size(); 
						if (maxRecord > 0) {
							ReferencedEnvelope r = collection.getBounds();
							center = "[" + String.valueOf(r.centre().getX()) + "," + String.valueOf(r.centre().getY()) + "]";
							envelope = "[[" + String.valueOf(r.getMinX()) + "," + String.valueOf(r.getMaxX()) + "]" +  ", [" + String.valueOf(r.getMinY()) + "," + String.valueOf(r.getMaxY()) + "]]";					
						}
						
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
						if (crs_target != null) {
							transformed = session.putAttribute(transformed, GeoAttributes.CRS.key(), crs_target.toWKT());
							transformed = session.putAttribute(transformed, GeoAttributes.GEO_CENTER.key(), center);
							transformed = session.putAttribute(transformed, GeoAttributes.GEO_ENVELOPE.key(), envelope);
							transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(),"application/avro+geowkt");								
						}
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
