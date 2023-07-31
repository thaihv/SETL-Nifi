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
package com.jdvn.setl.geos.wpsservice;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.eclipse.emf.common.util.EList;
import org.geotools.data.Parameter;
import org.geotools.data.wps.WebProcessingService;
import org.geotools.data.wps.request.DescribeProcessRequest;
import org.geotools.data.wps.response.DescribeProcessResponse;
import org.geotools.ows.ServiceException;
import org.geotools.text.Text;
import org.locationtech.jts.geom.Geometry;
import org.opengis.util.InternationalString;

import net.opengis.wps10.ComplexDataDescriptionType;
import net.opengis.wps10.DataInputsType;
import net.opengis.wps10.DescriptionType;
import net.opengis.wps10.InputDescriptionType;
import net.opengis.wps10.LiteralInputType;
import net.opengis.wps10.ProcessBriefType;
import net.opengis.wps10.ProcessDescriptionType;
import net.opengis.wps10.ProcessDescriptionsType;
import net.opengis.wps10.ProcessOfferingsType;
import net.opengis.wps10.SupportedComplexDataInputType;
import net.opengis.wps10.WPSCapabilitiesType;

@Tags({ "Geo Service", "WPS", "Process", "HTTP", "Map", "Vectors", "Rasters" })
@CapabilityDescription("Controller Service Implementation of WPS. It provides functions for handling requests and responses from WPS calls")
public class WPSStore extends AbstractControllerService implements WPSService {

	private WebProcessingService wps;
	private URL url;

	public static final PropertyDescriptor URL = new PropertyDescriptor.Builder().name("URL").description(
			"A http connection URL used to connect to a wps server. May contain basic name or host, port, and some parameters.")
			.defaultValue(null).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true)
			.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

	public static final PropertyDescriptor USER = new PropertyDescriptor.Builder().name("Database User")
			.description("Database user name").defaultValue(null).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

	public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder().name("Password")
			.description("The password for the database user").defaultValue(null).required(false).sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

	private List<PropertyDescriptor> properties;

	@Override
	protected void init(final ControllerServiceInitializationContext config) throws InitializationException {
		List<PropertyDescriptor> props = new ArrayList<>();
		props.add(URL);
		props.add(USER);
		props.add(PASSWORD);
		properties = Collections.unmodifiableList(props);
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	@Override
	public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {

	}

	@OnEnabled
	public void onEnabled(final ConfigurationContext context) throws InitializationException {
		final String szUrl = context.getProperty(URL).evaluateAttributeExpressions().getValue();
		try {
			this.url = new URL(szUrl);
			this.wps = new WebProcessingService(url);
		} catch (ServiceException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@OnDisabled
	public void shutdown() {

	}

	@Override
	public void execute() {

	}

	@SuppressWarnings("rawtypes")
	@Override
	public List<AllowableValue> getWPSCapabilities() {

		if (wps != null) {
			List<AllowableValue> listIdentifiers = new ArrayList<AllowableValue>();
			WPSCapabilitiesType capabilities = wps.getCapabilities();

			// view a list of processes offered by the server
			ProcessOfferingsType processOfferings = capabilities.getProcessOfferings();
			EList processes = processOfferings.getProcess();
			Iterator iterator = processes.iterator();
			while (iterator.hasNext()) {
				ProcessBriefType process = (ProcessBriefType) iterator.next();
				String value = process.getIdentifier().getValue();
				if (!(value.contains("JTS:") || value.contains("geo:"))) // ignore JTS and geo name space in geoserver because it just works for geometries not for features
					listIdentifiers.add(new AllowableValue(process.getIdentifier().getValue()));
			}
			return listIdentifiers;
		}
		return null;

	}
    @SuppressWarnings("rawtypes")
	private static Class getLiteralTypeFromReference(String ref) {
    	if (ref != null) {
            Class guess = guessLiteralType(ref);
            return guess != null ? guess : String.class; // default to string    		
    	}
    	return String.class;

    }

    /**
     * Try and parse a literaltype. If nothing matched, return null.
     *
     * @return class type it maps to, or null if no match was found
     */
    @SuppressWarnings("rawtypes")
	private static Class guessLiteralType(String s) {
        final String u = s.toUpperCase();

        if (u.contains("DOUBLE")) {
            return Double.class;
        } else if (u.contains("INTEGER")) {
            return Integer.class;
        } else if (u.contains("FLOAT")) {
            return Float.class;
        } else if (u.contains("BOOLEAN")) {
            return boolean.class;
        } else if (u.contains("CHAR")) {
            return Character.class;
        } else if (u.contains("STRING")) {
            return String.class;
        }

        return null;
    }	
    @SuppressWarnings("rawtypes")
	private static Class getComplexType(String encoding, String mimetype, String schema) {
        if ((encoding.toUpperCase()).contains("GML")
                || (mimetype.toUpperCase()).contains("GML")
                || (schema.toUpperCase()).contains("GML")) {
            return Geometry.class;
        } else if ((encoding.toUpperCase()).contains("POLYGON")
                || (mimetype.toUpperCase()).contains("POLYGON")
                || (schema.toUpperCase()).contains("POLYGON")) {
            return Geometry.class;
        } else if ((encoding.toUpperCase()).contains("POINT")
                || (mimetype.toUpperCase()).contains("POINT")
                || (schema.toUpperCase()).contains("POINT")) {
            return Geometry.class;
        } else if ((encoding.toUpperCase()).contains("LINE")
                || (mimetype.toUpperCase()).contains("LINE")
                || (schema.toUpperCase()).contains("LINE")) {
            return Geometry.class;
        } else if ((encoding.toUpperCase()).contains("RING")
                || (mimetype.toUpperCase()).contains("RING")
                || (schema.toUpperCase()).contains("RING")) {
            return Geometry.class;
        }

        // default to big O
        return Object.class;
    }	
    public static boolean isAbstractNull(DescriptionType description) {
        if (description.getAbstract() == null) {
            return true;
        }
        if (description.getAbstract().getValue() == null) {
            return true;
        }

        return false;
    }    
    @SuppressWarnings("rawtypes")
	public static Map<String, Parameter<?>> createInputParamMap(
            ProcessDescriptionType processDesc, Map<String, Parameter<?>> map) {
        if (map == null) {
            map = new TreeMap<>();
        }

        // loop through the process desc and setup each input param
        DataInputsType dataInputs = processDesc.getDataInputs();
        if (dataInputs == null) {
            return null;
        }

        EList inputs = dataInputs.getInput();
        if ((inputs == null) || inputs.isEmpty()) {
            return null;
        }

        Iterator iterator = inputs.iterator();
        while (iterator.hasNext()) {
            InputDescriptionType idt = (InputDescriptionType) iterator.next();
            // determine if the input is a literal or complex data, and from that
            // find out what type the object should be
            LiteralInputType literalData = idt.getLiteralData();
            SupportedComplexDataInputType complexData = idt.getComplexData();
            Class type = Object.class;
            if (literalData != null) {
            	String reference = literalData.getDataType() == null ? null: literalData.getDataType().getReference();
                type = getLiteralTypeFromReference(reference);
            } else if (complexData != null) {
                // TODO: get all supported types and determine how to handle that, not just the
                // default.
                ComplexDataDescriptionType format = complexData.getDefault().getFormat();
                String encoding = format.getEncoding();
                String mimetype = format.getMimeType();
                String schema = format.getSchema();
                if (encoding == null) {
                    encoding = "";
                }
                if (mimetype == null) {
                    mimetype = "";
                }
                if (schema == null) {
                    schema = "";
                }
                type = getComplexType(encoding, mimetype, schema);
            }

            // create the parameter
            boolean required = true;
            if (idt.getMinOccurs().intValue() < 1) {
                required = false;
            }

            String identifier = idt.getIdentifier().getValue();
            InternationalString title = Text.text(idt.getTitle().getValue());
            InternationalString description =
                    Text.text(isAbstractNull(idt) ? "" : idt.getAbstract().getValue());
            @SuppressWarnings("unchecked")
            Parameter<?> param =
                    new Parameter(
                            identifier,
                            type,
                            title,
                            description,
                            required,
                            idt.getMinOccurs().intValue(),
                            idt.getMaxOccurs().intValue(),
                            null,
                            null);
            map.put(identifier, param);
        }

        return map;
    }

	@Override
	public Map<String, Parameter<?>> getInputDataFromProcessIdentifier(String processIden) throws ServiceException, IOException {
		Map<String, Parameter<?>> params = new TreeMap<>();
		if (wps != null) {
			DescribeProcessRequest descRequest = wps.createDescribeProcessRequest();
			descRequest.setIdentifier(processIden); // describe the buffer process

			DescribeProcessResponse descResponse = wps.issueRequest(descRequest);
			ProcessDescriptionsType processDesc = descResponse.getProcessDesc();
			ProcessDescriptionType pdt = (ProcessDescriptionType) processDesc.getProcessDescription().get(0);
			
			params = createInputParamMap(pdt,params);
					
		}
		return params;

	}

	@Override
	public WebProcessingService getWps() {
		// TODO Auto-generated method stub
		return this.wps;
	}


}
