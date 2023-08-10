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
package com.jdvn.setl.geos.processors.geotext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.apache.nifi.components.AllowableValue;
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
import org.geotools.data.geojson.GeoJSONWriter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.kml.KMLConfiguration;
import org.geotools.referencing.CRS;
import org.geotools.wfs.GML;
import org.geotools.wfs.GML.Version;
import org.geotools.xsd.Encoder;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.jdvn.setl.geos.processors.util.GeoUtils;


@Tags({ "GML", "KML", "GeoJson", "Features", "Attributes", "Geospatial" })
@CapabilityDescription("Write geospatial data into a given formated file or flowfile.")
@SeeAlso({ GetXMLs.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class PutXMLs extends AbstractProcessor {

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
	public static final PropertyDescriptor B_TO_DIRECTORY = new PropertyDescriptor.Builder()
			.name("Write Data To Directory")
			.description("If true, write formated data to the given directory. If not, write data to flowfile to process.")
			.required(true)
			.allowableValues("true", "false")
			.defaultValue("false")
			.build();     
    
    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The directory to which xml files should be written. You may use expression language such as /aa/bb/${path}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(B_TO_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor MAX_DESTINATION_FILES = new PropertyDescriptor.Builder()
            .name("Maximum File Count")
            .description("Specifies the maximum number of files that can exist in the output directory")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .dependsOn(B_TO_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The character set of shapfiles to store")
            .required(false)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("UTF-8")
            .build();
    public static final PropertyDescriptor CRS_WKT = new PropertyDescriptor.Builder()
            .name("Coordinate Reference System OGC WKT")
            .description("The presentation of Coordinate Reference System in OGC Well-Known Text Format . If the number is 0, the Coordinate Reference System will be set same as CRS flowfile attribute")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(B_TO_DIRECTORY, "true")
            .build();    
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION)
            .dependsOn(B_TO_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor CHANGE_LAST_MODIFIED_TIME = new PropertyDescriptor.Builder()
            .name("Last Modified Time")
            .description("Sets the lastModifiedTime on the output file to the value of this attribute.  Format must be yyyy-MM-dd'T'HH:mm:ssZ.  "
                    + "You may also use expression language such as ${file.lastModifiedTime}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(B_TO_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor CHANGE_PERMISSIONS = new PropertyDescriptor.Builder()
            .name("Permissions")
            .description("Sets the permissions on the output file to the value of this attribute.  Format must be either UNIX rwxrwxrwx with a - in "
                    + "place of denied permissions (e.g. rw-r--r--) or an octal number (e.g. 644).  You may also use expression language such as "
                    + "${file.permissions}.")
            .required(false)
            .addValidator(PERMISSIONS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(B_TO_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor MERGE_PARTS = new PropertyDescriptor.Builder()
            .name("Merge Flowfiles")
            .description("Indicates whether or not merge flow files that have same Identifier into a shape file")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .dependsOn(B_TO_DIRECTORY, "true")
            .build();    
    public static final PropertyDescriptor CHANGE_OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .description("Sets the owner on the output file to the value of this attribute.  You may also use expression language such as "
                    + "${file.owner}. Note on many operating systems Nifi must be running as a super-user to have the permissions to set the file owner.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(B_TO_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor CHANGE_GROUP = new PropertyDescriptor.Builder()
            .name("Group")
            .description("Sets the group on the output file to the value of this attribute.  You may also use expression language such "
                    + "as ${file.group}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(B_TO_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .description("If true, then missing destination directories will be created. If false, flowfiles are penalized and sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .dependsOn(B_TO_DIRECTORY, "true")
            .build();
    public static final AllowableValue GML = new AllowableValue("GML", "GML", "Data in GML format");
    public static final AllowableValue KML = new AllowableValue("KML", "KML", "Data in KML format");
    public static final AllowableValue GEOJSON = new AllowableValue("Geojson", "Geojson", "Data in Geojson format");
    public static final PropertyDescriptor DATA_TYPE = new PropertyDescriptor.Builder()
            .name("type-geodata-to-write")
            .displayName("Written Data Type")
            .description("Which type of geo data to put. It could be GML, KML or Geojson")
            .required(true)
            .allowableValues(GML,KML,GEOJSON)
            .defaultValue(GML.getValue())
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
        supDescriptors.add(B_TO_DIRECTORY);
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
        supDescriptors.add(DATA_TYPE);
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

		final boolean b_todirectory = context.getProperty(B_TO_DIRECTORY).asBoolean();
		final String dataType  = context.getProperty(DATA_TYPE).evaluateAttributeExpressions().getValue();
		
		File targetFile;
		String rootname = flowFile.getAttributes().get(CoreAttributes.FILENAME.key());
		if (!FilenameUtils.getExtension(rootname).toUpperCase().equals(dataType))
		{
			String basePath = context.getProperty(DIRECTORY).getValue();
			targetFile = dataType.toUpperCase().equals("GEOJSON") == true ? new File(basePath + "/" + rootname + ".json") : new File(basePath + "/" + rootname + "." + dataType.toLowerCase()) ;
		}			
		else
			targetFile = new File(context.getProperty(DIRECTORY) + "/" + rootname);
		
		if (b_todirectory) {
			try {
				session.read(flowFile, new InputStreamCallback() {
					@Override
					public void process(final InputStream in) {
						try {
							AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(in);
							final String srs_source = flowFile.getAttributes().get(GeoAttributes.CRS.key());
							final CoordinateReferenceSystem crs_source = CRS.parseWKT(srs_source);
							SimpleFeatureCollection featureCollection = GeoUtils.createSimpleFeatureCollectionFromNifiRecords("featurecollection", reader, crs_source, null);

				            if (dataType.equals("GML")) {
				                GML encode = new GML(Version.WFS1_1);
				                encode.setNamespace("geotools", "http://geotools.org");		
				        		try (FileOutputStream output = new FileOutputStream(targetFile)) {
				        			encode.encode(output, featureCollection );
				        		} catch (IOException e) {
				        			e.printStackTrace();
				        		}
				            }
				            else if (dataType.equals("KML")){		            	
				        		Encoder encoder = new Encoder(new KMLConfiguration());
				        		encoder.setIndenting(true);
				        		try (FileOutputStream output = new FileOutputStream(targetFile)) {
				        			encoder.encode(featureCollection, org.geotools.kml.KML.kml, output);
				        		} catch (IOException e) {
				        			e.printStackTrace();
				        		}
				            }
				            else { // case of Geojson
				        		try (FileOutputStream output = new FileOutputStream(targetFile)) {
				        			GeoJSONWriter w = new GeoJSONWriter(output);
				        			w.writeFeatureCollection(featureCollection);
				        			w.close();

				        		} catch (IOException e) {
				        			e.printStackTrace();
				        		}
				            }

							session.adjustCounter("Records Written", featureCollection.size(), false);
						} catch (IOException | FactoryException e) {
							logger.error("Could not save {} because {}", new Object[] { flowFile, e });
							session.transfer(flowFile, REL_FAILURE);
							return;
						}
					}
				});
				session.remove(flowFile);
			} catch (Exception e) {
				logger.error("Could not save {} because {}", new Object[] { flowFile, e });
				session.transfer(flowFile, REL_FAILURE);
				return;
			}
		} else {
			try {
				session.read(flowFile, new InputStreamCallback() {
					@Override
					public void process(final InputStream in) {
						try {
							AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(in);
							final String srs_source = flowFile.getAttributes().get(GeoAttributes.CRS.key());
							String geoName = flowFile.getAttributes().get(CoreAttributes.FILENAME.key());
							final CoordinateReferenceSystem crs_source = CRS.parseWKT(srs_source);
							SimpleFeatureCollection featureCollection = GeoUtils.createSimpleFeatureCollectionFromNifiRecords("featurecollection", reader, crs_source, null);

							ByteArrayOutputStream xml_out = new ByteArrayOutputStream();							
				            if (dataType.equals("GML")) {				            					        		
				            	GML encode = new GML(Version.WFS1_1);
				            	encode.setNamespace("geotools", "http://geotools.org");
				            	encode.encode(xml_out,  featureCollection);
				            }
				            else if (dataType.equals("KML")){		            	
				        		Encoder encoder = new Encoder(new KMLConfiguration());
				        		encoder.setIndenting(true);
				        		encoder.encode(featureCollection, org.geotools.kml.KML.kml, xml_out);
				            }
				            else { // case of Geojson
			        			GeoJSONWriter w = new GeoJSONWriter(xml_out);
			        			w.writeFeatureCollection(featureCollection);
			        			w.close();
				            }
				            if (xml_out != null) {
								FlowFile transformed = session.create();
				                try (ByteArrayInputStream xml_in = new ByteArrayInputStream(xml_out.toByteArray())) {
									session.importFrom(xml_in, transformed);		
									xml_out.close();
				                }
								transformed = session.putAttribute(transformed, GeoUtils.GEO_DB_SRC_TYPE, dataType);
								transformed = session.putAttribute(transformed, GeoAttributes.GEO_NAME.key(), geoName);
								transformed = session.putAttribute(transformed, CoreAttributes.FILENAME.key(), geoName);
								transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), dataType.toUpperCase().equals("GEOJSON") == true ? "application/json": "application/xml");
								session.getProvenanceReporter().receive(transformed, geoName, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
								logger.info("added {} to flow", new Object[] { transformed });
								session.adjustCounter("Data Read", xml_out.size(), false);
								session.transfer(transformed, REL_SUCCESS);
				            }

							session.adjustCounter("Data Written", xml_out.size(), false);
						} catch (IOException | FactoryException e) {
							logger.error("Could not save {} because {}", new Object[] { flowFile, e });
							session.transfer(flowFile, REL_FAILURE);
							return;
						}
					}
				});
				session.remove(flowFile);

			} catch (Exception e) {
				logger.error("Could not save {} because {}", new Object[] { flowFile, e });
				session.transfer(flowFile, REL_FAILURE);
				return;
			}
		}
	}

}
