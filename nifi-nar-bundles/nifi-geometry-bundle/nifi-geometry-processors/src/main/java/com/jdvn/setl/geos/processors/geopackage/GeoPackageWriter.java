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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"OGC Geopackage", "wkt", "json", "Tiles", "Features", "Coordinate System", "Attributes", "Geospatial"})
@CapabilityDescription("Write geospatial data into geopackage.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GeoPackageWriter extends AbstractProcessor {

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";

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
    public static final PropertyDescriptor GEOPACKAGE_FILE_NAME = new PropertyDescriptor.Builder()
            .name("Geopackage file name")
            .description("The geopackage file should be written. You may use expression language such as /aa/bb/${path}")
            .required(true)
            .defaultValue("mygeopackage.gpkg")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("CharSet")
            .description("The name of charset for attributes in target shapfiles")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
        supDescriptors.add(GEOPACKAGE_FILE_NAME);
        supDescriptors.add(CHARSET);
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
        final long importStart = System.nanoTime();
        final ComponentLog logger = getLogger();
        
		File targetFile = new File(context.getProperty(GEOPACKAGE_FILE_NAME).evaluateAttributeExpressions(flowFile).getValue());
		String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue();
		try {
			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(final InputStream in) {

				}
			});

		} catch (Exception e) {
			logger.error("Could not save {} because {}", new Object[]{flowFile, e});
			session.transfer(flowFile, REL_FAILURE);
			return;
		}
        final long importNanos = System.nanoTime() - importStart;
        final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
		session.getProvenanceReporter().receive(flowFile, targetFile.toURI().toString(), importMillis);
		session.transfer(flowFile, REL_SUCCESS);
	}
}
