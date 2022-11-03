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
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.flowfile.attributes.GeoAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.jdvn.setl.geos.processors.util.GeoUtils;


@Tags({ "Shape file", "wkt", "json",  "Features", "Attributes", "Geospatial" })
@CapabilityDescription("Write geospatial data into a given shape file.")
@SeeAlso({ ShpReader.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class ShpWriter extends AbstractProcessor {

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    
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
            .description("The directory to which shape files should be written. You may use expression language such as /aa/bb/${path}")
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
            .name("Character Set")
            .description("The name of charset for attributes in target shapfiles")
            .required(true)
            .defaultValue("ISO-8859-1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CRS_WKT = new PropertyDescriptor.Builder()
            .name("Coordinate Reference System OGC WKT")
            .description("The presentation of Coordinate Reference System in OGC Well-Known Text Format . If the number is 0, the Coordinate Reference System will be set same as CRS flowfile attribute")
            .required(true)
            .defaultValue("0")
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
    public static final PropertyDescriptor MERGE_PARTS = new PropertyDescriptor.Builder()
            .name("Merge Flowfiles")
            .description("Indicates whether or not merge flow files that have same Identifier into a shape file")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
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
        supDescriptors.add(CRS_WKT);
        supDescriptors.add(CREATE_DIRS);
        supDescriptors.add(MERGE_PARTS);
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
		final StopWatch stopWatch = new StopWatch(true);
        final ComponentLog logger = getLogger();
        
        String rootname = flowFile.getAttributes().get(CoreAttributes.FILENAME.key());
        if (FilenameUtils.getExtension(rootname).contains("shp") == true)
        	rootname = rootname.substring(0, rootname.length() - 4);
        
        String geoname = flowFile.getAttributes().get(GeoAttributes.GEO_NAME.key());     
        String part_name = ".shp";
        if (geoname != null) {
        	if (geoname.indexOf(":") != -1)
        		rootname = geoname.substring(0, geoname.indexOf(":"));
        	else 
        		rootname = geoname;
        	if (geoname.lastIndexOf(":") != -1)
        		part_name  = "_" + geoname.substring(geoname.lastIndexOf(":") + 1) + ".shp";
        }

        String fragmentIdentifier = flowFile.getAttributes().get(FRAGMENT_ID) == null ? "shapefile" : flowFile.getAttributes().get(FRAGMENT_ID);
        

        final boolean merged = context.getProperty(MERGE_PARTS).asBoolean();
        String filename = merged ? rootname + ".shp" : rootname + part_name;
        
        final File srcFile = new File(context.getProperty(DIRECTORY) + "/" + filename);
        final String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue();
        final String srs_wkt = context.getProperty(CRS_WKT).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[\\r\\n\\t ]", "");
		try {
			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(final InputStream in) {
					try {
						AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(in);
						final String srs_source = flowFile.getAttributes().get(GeoAttributes.CRS.key());
						final CoordinateReferenceSystem crs_source = CRS.parseWKT(srs_source);
						CoordinateReferenceSystem crs_target = crs_source;
						if (!srs_wkt.equals("0")) {
							try {
								crs_target = CRS.parseWKT(srs_wkt);
							} catch (FactoryException e) {
								logger.error("Unable to create the CRS from {}, We will use CRS from flowfile. Verify your CRS Well-Known Text!", new Object[]{srs_wkt});
								crs_target = crs_source;
							}
						}				
						SimpleFeatureCollection collection = GeoUtils.createSimpleFeatureCollectionFromNifiRecords(fragmentIdentifier, reader, crs_source, crs_target);
						if (createShapeFileFromGeoDataFlowfile(srcFile, charset, collection))
							logger.info("Saved {} to file {}", new Object[]{flowFile, srcFile.toURI().toString()});
						else {
							logger.info("Unable to create the shape file {}", new Object[]{srcFile.toURI().toString()});
						}	
						session.adjustCounter("Records performed", collection.size(), false);
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
			return;
		} 

		session.getProvenanceReporter().send(flowFile, srcFile.toURI().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
		session.transfer(flowFile, REL_SUCCESS);
	}
	public boolean createShapeFileFromGeoDataFlowfile(File srcFile, String charsetName, SimpleFeatureCollection collection) {
		final ComponentLog logger = getLogger();
		SimpleFeatureType schema = null;
		if (collection.features().hasNext())
			schema = collection.features().next().getFeatureType();
		else
			return false;
		
		ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
		Map<String, Serializable> params = new HashMap<>();
		Transaction transaction = null;
		try {
			ShapefileDataStore newDataStore;
			if(!srcFile.exists()) { 
				params.put("url", srcFile.toURI().toURL());
				params.put("create spatial index", Boolean.TRUE);
				newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
				newDataStore.setCharset(Charset.forName(charsetName));
				newDataStore.createSchema(schema);
			}else {
				params.put("url", srcFile.toURI().toURL());
				DataStore dataStore = DataStoreFinder.getDataStore(params);
				newDataStore = (ShapefileDataStore) dataStore;
			} 
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
				transaction.close();
				return false;
			}
		} catch (IOException e) {
			logger.error("Could not create Shape File because {}", new Object[]{e});
			return false;
		} 
		return true;
	} 
}
