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
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
import org.apache.nifi.util.StopWatch;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureWriter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.geopkg.FeatureEntry;
import org.geotools.geopkg.GeoPackage;
import org.geotools.geopkg.Tile;
import org.geotools.geopkg.TileEntry;
import org.geotools.geopkg.TileMatrix;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
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
    public static final PropertyDescriptor MERGE_PARTS = new PropertyDescriptor.Builder()
            .name("Merge Flowfiles")
            .description("Indicates whether or not merge flow files that have same Identifier into a feature entry in Geopackage")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
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
        supDescriptors.add(MERGE_PARTS);
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
		final boolean merged = context.getProperty(MERGE_PARTS).asBoolean();
		
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
	                    	final StopWatch stopWatch = new StopWatch(true);
	                    	final String src_url = flowFile.getAttribute(GeoUtils.GEO_URL) != null ? flowFile.getAttribute(GeoUtils.GEO_URL) : "GeoPackage";	                        
	                    	getLogger().info("Get flowfile {} is ok!", flowFile);
	            			session.read(flowFile, new InputStreamCallback() {
	            				@Override
	            				public void process(final InputStream in) {
	            					try {
	            						String geoType = flowFile.getAttribute(GeoAttributes.GEO_TYPE.key());
	        	                		String geoname = flowFile.getAttributes().get(GeoAttributes.GEO_NAME.key());
	        	                		String rootname = geoname;
	        	                		String part_name = "";
	        	                        if (geoname != null) {
	        	                        	if (geoname.indexOf(":") != -1)
	        	                        		rootname = geoname.substring(0, geoname.indexOf(":"));
	        	                        	else 
	        	                        		rootname = geoname;
	        	                        	if (geoname.lastIndexOf(":") != -1)
	        	                        		part_name  = "_" + geoname.substring(geoname.lastIndexOf(":") + 1);
	        	                        }
	        	                        String entryname = merged ? rootname : rootname + part_name;
	        	                        
	            						if (geoType.contains("Features")){
	            							FeatureEntry entry;
	            							final String srs = flowFile.getAttributes().get(GeoAttributes.CRS.key());
	            							CoordinateReferenceSystem crs_source = CRS.parseWKT(srs);
		            						AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(in);
		            						SimpleFeatureCollection collection = GeoUtils.createSimpleFeatureCollectionFromNifiRecords(entryname, reader, crs_source, null);
	            							if (geopkg.feature(entryname) == null) {
	            								entry = new FeatureEntry();
			            						Integer srid = CRS.lookupEpsgCode(crs_source, true);
			            						if (srid != null)
			            							entry.setSrid(srid);
			            						else
			            							entry.setSrid(Integer.valueOf(epsg));
			            						entry.setDescription(entryname);
			            						geopkg.add(entry, collection);
			            						geopkg.createSpatialIndex(entry);
	            							}else {
	            								entry = geopkg.feature(entryname);
		            					        Transaction tx = new DefaultTransaction();		            					
		            					        try {
		            					            try (SimpleFeatureWriter w = geopkg.writer(entry, true, null, tx);
		            					                    SimpleFeatureIterator it = collection.features()) {
		            					                while (it.hasNext()) {
		            					                    SimpleFeature f = it.next();
		            					                    SimpleFeature g = w.next();
		            					                    g.setAttributes(f.getAttributes());
		            					                    for (org.opengis.feature.type.PropertyDescriptor pd : collection.getSchema().getDescriptors()) {
		            					                        /* geopkg spec requires booleans to be stored as SQLite integers */
		            					                        String name = pd.getName().getLocalPart();
		            					                        if (pd.getType().getBinding() == Boolean.class) {
		            					                            int bool = 0;
		            					                            if (f.getAttribute(name) != null) {
		            					                                bool = (Boolean) (f.getAttribute(name)) ? 1 : 0;
		            					                            }
		            					                            g.setAttribute(name, bool);
		            					                        }
		            					                    }
		            					                    w.write();
		            					                }
		            					            }
		            					            tx.commit();
		            					        } catch (Exception ex) {
		            					            tx.rollback();
		            					            throw new IOException(ex);
		            					        } finally {
		            					            tx.close();
		            					        }
	            							}
	            							session.adjustCounter("Records Written", collection.size(), false);

	            						}
	            						else if (geoType.contains("Tiles")) {
	            							TileEntry e;
	            							final String srs = flowFile.getAttributes().get(GeoAttributes.CRS.key());
	            							final String len = flowFile.getAttributes().get(GeoUtils.GEO_TTLE_MATRIX_BYTES_LEN);
	            							final String encodedTileMatrices = flowFile.getAttributes().get(GeoAttributes.GEO_TILE_MATRIX.key());
	            							
	            							String envelope = flowFile.getAttributes().get(GeoAttributes.GEO_ENVELOPE.key());	            							
            								envelope = envelope.substring(1, envelope.length() - 1);
            								List<String> xy = Arrays.asList(envelope.split(","));					
            								double x1 = Double.valueOf(xy.get(0).trim().replace("[", ""));
            								double x2 = Double.valueOf(xy.get(1).trim().replace("]", ""));
            								double y1 = Double.valueOf(xy.get(2).trim().replace("[", ""));
            								double y2 = Double.valueOf(xy.get(3).trim().replace("]", ""));
            									            							
	            							byte[] decodedListTileMatrices = Base64.getDecoder().decode(encodedTileMatrices);
	            							
	            							List<TileMatrix> listTileMatices= GeoUtils.unzipTileMatrixFromBytes(decodedListTileMatrices, Integer.valueOf(len));
	            							CoordinateReferenceSystem crs_source = CRS.parseWKT(srs);
		            						AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(in);
		            						List<Tile> tiles = GeoUtils.getTilesFromNifiRecords(entryname, reader, crs_source);
	            							if (geopkg.tile(entryname) == null) {
		            							e = new TileEntry();
		            							e.setTableName(entryname);
		            							e.setBounds(new ReferencedEnvelope(x1,x2,y1,y2,crs_source));
		            							for (int i = 0; i < listTileMatices.size(); i++)
		            								e.getTileMatricies().add(listTileMatices.get(i));
		            							geopkg.create(e);	            								
	            							}
	            							else
	            								e = geopkg.tile(entryname);
	            							for (Tile t : tiles) {
	            							    geopkg.add(e, t);
	            							}
	            							getLogger().info("The flowfile {} is Tiles!", flowFile);
	            							session.adjustCounter("Records Written", tiles.size(), false);
	            						}


	            					} catch (IOException | FactoryException e) {
	            						getLogger().error("Failed to bin {} due to {}", flowFile, e, e);
	            						session.transfer(flowFile, REL_FAILURE);
	            						return;
	            					}
	            				}
	            			});                    	
	                    	
	            			session.getProvenanceReporter().send(flowFile, src_url, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
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
