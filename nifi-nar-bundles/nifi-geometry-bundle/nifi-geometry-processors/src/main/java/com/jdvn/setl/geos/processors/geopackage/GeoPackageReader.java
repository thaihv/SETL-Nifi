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
import java.util.ArrayList;
import java.util.Collections;
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

import mil.nga.geopackage.GeoPackage;
import mil.nga.geopackage.GeoPackageManager;
import mil.nga.geopackage.contents.ContentsDao;
import mil.nga.geopackage.extension.ExtensionsDao;
import mil.nga.geopackage.extension.metadata.MetadataDao;
import mil.nga.geopackage.extension.metadata.MetadataExtension;
import mil.nga.geopackage.extension.metadata.reference.MetadataReferenceDao;
import mil.nga.geopackage.extension.schema.SchemaExtension;
import mil.nga.geopackage.extension.schema.columns.DataColumnsDao;
import mil.nga.geopackage.extension.schema.constraints.DataColumnConstraintsDao;
import mil.nga.geopackage.features.columns.GeometryColumnsDao;
import mil.nga.geopackage.features.user.FeatureDao;
import mil.nga.geopackage.features.user.FeatureResultSet;
import mil.nga.geopackage.geom.GeoPackageGeometryData;
import mil.nga.geopackage.srs.SpatialReferenceSystemDao;
import mil.nga.geopackage.tiles.matrix.TileMatrixDao;
import mil.nga.geopackage.tiles.matrixset.TileMatrixSetDao;
import mil.nga.sf.Geometry;
import mil.nga.sf.wkt.GeometryWriter;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
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
     // Open a GeoPackage
        GeoPackage geoPackage = GeoPackageManager.open(file);

        // GeoPackage Table DAOs
        SpatialReferenceSystemDao srsDao = geoPackage.getSpatialReferenceSystemDao();
        ContentsDao contentsDao = geoPackage.getContentsDao();
        GeometryColumnsDao geomColumnsDao = geoPackage.getGeometryColumnsDao();
        TileMatrixSetDao tileMatrixSetDao = geoPackage.getTileMatrixSetDao();
        TileMatrixDao tileMatrixDao = geoPackage.getTileMatrixDao();
        SchemaExtension schemaExtension = new SchemaExtension(geoPackage);
        DataColumnsDao dataColumnsDao = schemaExtension.getDataColumnsDao();
        DataColumnConstraintsDao dataColumnConstraintsDao = schemaExtension.getDataColumnConstraintsDao();
        MetadataExtension metadataExtension = new MetadataExtension(geoPackage);
        MetadataDao metadataDao = metadataExtension.getMetadataDao();
        MetadataReferenceDao metadataReferenceDao = metadataExtension.getMetadataReferenceDao();
        ExtensionsDao extensionsDao = geoPackage.getExtensionsDao();

        // Feature and tile tables
        List<String> features = geoPackage.getFeatureTables();
        List<String> tiles = geoPackage.getTileTables();

        // Query Features
        String featureTable = features.get(0);
        FeatureDao featureDao = geoPackage.getFeatureDao(featureTable);
        FeatureResultSet featureResultSet = featureDao.queryForAll();
        
        for (int i=0; i < featureResultSet.getCount(); i++ ) {
        	GeoPackageGeometryData geometryData = featureResultSet.getRow().getGeometry();
            if (geometryData != null && !geometryData.isEmpty()) {
                Geometry geometry = geometryData.getGeometry();
                try {
					System.out.print(GeometryWriter.writeGeometry(geometry));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

              }
        }

        
    }
}
