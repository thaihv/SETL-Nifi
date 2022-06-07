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

import java.awt.Rectangle;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.geopkg.GeoPackage;
import org.geotools.geopkg.GeoPkgDataStoreFactory;
import org.geotools.geopkg.TileEntry;
import org.geotools.parameter.Parameter;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

@Tags({ "example" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class GeoPackageReader extends AbstractProcessor {

	static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder().name("File to Fetch")
			.description("The fully-qualified filename of the file to fetch from the file system")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.defaultValue("${absolute.path}/${filename}").required(true).build();

	static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("Shape file is routed to success").build();
	static final Relationship REL_NOT_FOUND = new Relationship.Builder().name("not.found").description(
			"Any FlowFile that could not be fetched from the file system because the file could not be found will be transferred to this Relationship.")
			.build();
	static final Relationship REL_PERMISSION_DENIED = new Relationship.Builder().name("permission.denied").description(
			"Any FlowFile that could not be fetched from the file system due to the user running NiFi not having sufficient permissions will be transferred to this Relationship.")
			.build();
	static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description(
			"Any FlowFile that could not be fetched from the file system for any reason other than insufficient permissions or the file not existing will be transferred to this Relationship.")
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
		relationships.add(REL_NOT_FOUND);
		relationships.add(REL_PERMISSION_DENIED);
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
		FlowFile flowFile = session.get();
		// TODO implement
		final StopWatch stopWatch = new StopWatch(true);
		final String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
		final File file = new File(filename);

		HashMap<String, Object> map = new HashMap<>();
		map.put(GeoPkgDataStoreFactory.DBTYPE.key, "geopkg");
		map.put(GeoPkgDataStoreFactory.DATABASE.key, filename);
		try {
			DataStore store = DataStoreFinder.getDataStore(map);
			String[] names = store.getTypeNames();
			for (String name : names) {
				System.out.println(name);
				SimpleFeatureCollection features = store.getFeatureSource(name).getFeatures();
				try (SimpleFeatureIterator itr = features.features()) {

					int count = 0;
					while (itr.hasNext() && count < 10) {
						SimpleFeature f = itr.next();
						System.out.println(f);
						count++;
					}
				}
			}
			store.dispose();
		} catch (IOException e) {
			e.printStackTrace();
		}

		GeoPackage geoPackage;
		try {
			geoPackage = new GeoPackage(file);
			GeneralParameterValue[] parameters = new GeneralParameterValue[1];
			for (TileEntry tileEntry : geoPackage.tiles()) {

				ReferencedEnvelope referencedEnvelope = tileEntry.getBounds();
				Rectangle rectangle = new Rectangle((int) referencedEnvelope.getWidth(),
						(int) referencedEnvelope.getHeight());
				GridEnvelope2D gridEnvelope = new GridEnvelope2D(rectangle);
				GridGeometry2D gridGeometry = new GridGeometry2D(gridEnvelope, referencedEnvelope);
				parameters[0] = new Parameter<GridGeometry2D>(AbstractGridFormat.READ_GRIDGEOMETRY2D, gridGeometry);
				String tableName = tileEntry.getTableName();
				System.out.println(tableName);

				org.geotools.geopkg.mosaic.GeoPackageReader reader = new org.geotools.geopkg.mosaic.GeoPackageReader(file, null);
				System.out.println(Arrays.asList(reader.getGridCoverageNames()));

				GridCoverage2D gridCoverage = reader.read(tableName, parameters);
				RenderedImage img = gridCoverage.getRenderedImage();
				System.out.println(img);
				reader.dispose();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		

	}

	public CoordinateReferenceSystem getCRSFromGeopackageTable(final File geopkg, String tableName) {
		HashMap<String, Object> map = new HashMap<>();
		map.put(GeoPkgDataStoreFactory.DBTYPE.key, "geopkg");
		map.put(GeoPkgDataStoreFactory.DATABASE.key, geopkg);

		CoordinateReferenceSystem cRS = null;
		try {
			DataStore store = DataStoreFinder.getDataStore(map);
			SimpleFeatureCollection features = store.getFeatureSource(tableName).getFeatures();
			SimpleFeatureType schema = features.getSchema();
			cRS = schema.getCoordinateReferenceSystem();
			store.dispose();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return cRS;
	}
}
