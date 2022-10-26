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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.GeoAttributes;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.geopkg.FeatureEntry;
import org.geotools.geopkg.GeoPackage;
import org.geotools.geopkg.Tile;
import org.geotools.geopkg.TileEntry;
import org.geotools.geopkg.TileMatrix;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.jdvn.setl.geos.processors.util.GeoUtils;

@Tags({"OGC Geopackage", "wkt", "json", "Tiles", "Features", "Coordinate System", "Attributes", "Geospatial"})
@CapabilityDescription("Write geospatial data into geopackage.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GeoPackageWriter extends AbstractSessionFactoryProcessor {
    public static final PropertyDescriptor GEOPACKAGE_FILE_NAME = new PropertyDescriptor.Builder()
            .name("Geopackage file name")
            .description("The geopackage file should be written. You may use expression language such as /aa/bb/${path}")
            .required(true)
            .defaultValue("mygeopackage.gpkg")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor SRID = new PropertyDescriptor.Builder()
            .name("Default EPSG")
            .description("The number value of EPSG using for entries of Geopackage when no Reference System Identifier has been found from flow file")
            .required(true)
            .defaultValue("4326")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

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
        supDescriptors.add(GEOPACKAGE_FILE_NAME);
        supDescriptors.add(SRID);
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
	public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
    	
		File targetFile = new File(context.getProperty(GEOPACKAGE_FILE_NAME).evaluateAttributeExpressions().getValue());
		String epsg = context.getProperty(SRID).evaluateAttributeExpressions().getValue();
		
		try {
			final GeoPackage geopkg = new GeoPackage(targetFile);
	    	geopkg.init();
	        while (isScheduled()) {
	            final ProcessSession session = sessionFactory.createSession();
	            final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(550, DataUnit.KB, 250));
	            if (flowFiles.isEmpty()) {
	                break;
	            }

	            if (getLogger().isDebugEnabled()) {
	                final List<String> ids = flowFiles.stream().map(ff -> "id=" + ff.getId()).collect(Collectors.toList());
	                getLogger().debug("Pulled {} FlowFiles from queue: {}", ids.size(), ids);
	            }

	            try {
	                for (final FlowFile flowFile : flowFiles) {
	                    try {
	                    	getLogger().info("Get flowfile {} is ok!", flowFile);
	            			session.read(flowFile, new InputStreamCallback() {
	            				@Override
	            				public void process(final InputStream in) {
	            					try {
	            						String geoType = flowFile.getAttribute(GeoAttributes.GEO_TYPE.key());
	            						String geoName = flowFile.getAttribute(GeoAttributes.GEO_NAME.key());
	            						if (geoType.contains("Features")){
		            						FeatureEntry entry = new FeatureEntry();
		            						final String srs = flowFile.getAttributes().get(GeoAttributes.CRS.key());
		            						CoordinateReferenceSystem crs_source = CRS.parseWKT(srs);
		            						Integer srid = CRS.lookupEpsgCode(crs_source, true);
		            						if (srid != null)
		            							entry.setSrid(srid);
		            						else
		            							entry.setSrid(Integer.valueOf(epsg));
		            						entry.setDescription(geoName);
		            						
		            						AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(in);
		            						SimpleFeatureCollection collection = GeoUtils.createSimpleFeatureCollectionFromNifiRecords(geoName, reader, crs_source, null);
		            						geopkg.add(entry, collection);
		            						geopkg.createSpatialIndex(entry);
	            						}
	            						else if (geoType.contains("Tiles")) {
	            							
	            							TileEntry e = new TileEntry();
	            							e.setTableName(geoName);
	            							e.setBounds(new ReferencedEnvelope(-180,180,-90,90,DefaultGeographicCRS.WGS84));
	            							e.getTileMatricies().add(new TileMatrix(0, 1, 1, 256, 256, 0.1, 0.1));
	            							e.getTileMatricies().add(new TileMatrix(1, 2, 2, 256, 256, 0.1, 0.1));

	            							geopkg.create(e);

	            							List<Tile> tiles = new ArrayList();
	            							tiles.add(new Tile(0,0,0,new byte[]{3,4,6}));
	            							tiles.add(new Tile(1,0,0,new byte[]{15,76,3}));
	            							tiles.add(new Tile(1,0,1,new byte[]{4,5,2}));
	            							tiles.add(new Tile(1,1,0,new byte[]{67,8,23}));
	            							tiles.add(new Tile(1,1,1,new byte[]{4,6,8}));

	            							for (Tile t : tiles) {
	            							    geopkg.add(e, t);
	            							}
	            							
	            							getLogger().info("The flowfile {} is Tiles!", flowFile);
	            						}


	            					} catch (IOException | FactoryException e) {
	            						getLogger().error("Failed to bin {} due to {}", flowFile, e, e);
	            						session.transfer(flowFile, REL_FAILURE);
	            						return;
	            					}
	            				}
	            			});                    	
	                    	
	                    	
	                    	session.transfer(flowFile, REL_SUCCESS);
	                    	
	                    } catch (final Exception e) {
	                        getLogger().error("Failed to bin {} due to {}", flowFile, e, e);
	                        session.transfer(flowFile, REL_FAILURE);
	                    }
	                    
	                }
	                
	            } finally {
	                session.commitAsync();
	                
	            }
	        }
	        geopkg.close();
	
		} catch (IOException e1) {

			e1.printStackTrace();
		}
	}
}
