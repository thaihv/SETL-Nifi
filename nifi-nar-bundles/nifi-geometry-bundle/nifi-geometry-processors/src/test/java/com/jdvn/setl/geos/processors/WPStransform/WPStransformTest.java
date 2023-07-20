
package com.jdvn.setl.geos.processors.WPStransform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.xml.namespace.QName;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.geotools.data.wps.WPSFactory;
import org.geotools.data.wps.WPSUtils;
import org.geotools.data.wps.WebProcessingService;
import org.geotools.data.wps.request.DescribeProcessRequest;
import org.geotools.data.wps.request.ExecuteProcessRequest;
import org.geotools.data.wps.response.DescribeProcessResponse;
import org.geotools.data.wps.response.ExecuteProcessResponse;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.http.HTTPClient;
import org.geotools.http.HTTPResponse;
import org.geotools.http.SimpleHttpClient;
import org.geotools.ows.ServiceException;
import org.geotools.process.Process;
import org.geotools.process.ProcessException;
import org.geotools.wps.WPS;
import org.geotools.wps.WPSConfiguration;
import org.geotools.xsd.Configuration;
import org.geotools.xsd.Encoder;
import org.geotools.xsd.EncoderDelegate;
import org.geotools.xsd.Parser;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.xml.sax.ContentHandler;
import org.xml.sax.ext.LexicalHandler;

import net.opengis.ows11.CodeType;
import net.opengis.ows11.DomainMetadataType;
import net.opengis.ows11.ExceptionReportType;
import net.opengis.ows11.Ows11Factory;
import net.opengis.wfs.FeatureCollectionType;
import net.opengis.wps10.ComplexDataType;
import net.opengis.wps10.DataInputsType1;
import net.opengis.wps10.DataType;
import net.opengis.wps10.ExecuteResponseType;
import net.opengis.wps10.ExecuteType;
import net.opengis.wps10.InputDescriptionType;
import net.opengis.wps10.InputReferenceType;
import net.opengis.wps10.InputType;
import net.opengis.wps10.MethodType;
import net.opengis.wps10.OutputDataType;
import net.opengis.wps10.OutputDefinitionType;
import net.opengis.wps10.ProcessBriefType;
import net.opengis.wps10.ProcessDescriptionType;
import net.opengis.wps10.ProcessDescriptionsType;
import net.opengis.wps10.ProcessOfferingsType;
import net.opengis.wps10.ResponseDocumentType;
import net.opengis.wps10.ResponseFormType;
import net.opengis.wps10.ValuesReferenceType;
import net.opengis.wps10.WPSCapabilitiesType;
import net.opengis.wps10.Wps10Factory;

public class WPStransformTest {
	private WebProcessingService wps;
	private URL url;
	private String processIden;
	HTTPClient httpClient;
	
	protected String getFixtureId() {
		return "wps";
	}

	protected Properties createExampleFixture() {
		Properties example = new Properties();
		example.put("service", "http://localhost:8088/geoserver/ows?service=wps&version=1.0.0&request=GetCapabilities");
		example.put("processId", "geo:buffer");
		return example;
	}

	public void connect() throws ServiceException, IOException {

		// local server
		String serviceProperty = createExampleFixture().getProperty("service");
		if (serviceProperty == null) {
			throw new ServiceException("Service URL not provided by test fixture");
		}
		url = new URL(serviceProperty);
		processIden = createExampleFixture().getProperty("processId");
		wps = new WebProcessingService(url /* capabilities */);
	}

	@Before
	public void init() throws ServiceException, IOException {
		connect();
	}
    private void setLocalInputDataBufferPoly(
            ExecuteProcessRequest exeRequest, ProcessDescriptionsType processDesc, Geometry geom)
            throws ParseException {

        // this process takes 2 input, a geometry and a buffer amount.
        ProcessDescriptionType pdt = (ProcessDescriptionType) processDesc.getProcessDescription().get(0);
        
        System.out.println("INPUTS FOR BUFFER OPERATION:");
        for (int i = 0; i < pdt.getDataInputs().getInput().size(); i++ ) {
        	InputDescriptionType idt = (InputDescriptionType) pdt.getDataInputs().getInput().get(i);
//        	if (idt.getLiteralData() != null)
//        		if (idt.getLiteralData().getDataType() != null)
//        			idt.getLiteralData().getDataType().setReference("3");
//        		else {
//        			DomainMetadataType dm = net.opengis.ows11.Ows11Factory.eINSTANCE.createDomainMetadataType();
//        			idt.getLiteralData().setDataType(dm);
//        			idt.getLiteralData().getDataType().setReference("3");
//        		}
        			
        	System.out.println(idt.getIdentifier().getValue());
        }
        InputDescriptionType idt = (InputDescriptionType) pdt.getDataInputs().getInput().get(0);
        // create input buffer
        int bufferAmnt = 350;

        // create and set the input on the exe request
        if (idt.getIdentifier().getValue().equalsIgnoreCase("distance")) {
            // set buffer input
            DataType input = WPSUtils.createInputDataType(bufferAmnt, idt);
            List<EObject> list = new ArrayList<>();
            list.add(input);
            exeRequest.addInput(idt.getIdentifier().getValue(), list);
            // set geom input
            idt = (InputDescriptionType) pdt.getDataInputs().getInput().get(1);

            DataType input2 = WPSUtils.createInputDataType(geom, idt);
            List<EObject> list2 = new ArrayList<>();
            list2.add(input2);
            exeRequest.addInput(idt.getIdentifier().getValue(), list2);
        } else {
            // set geom input
            DataType input2 = WPSUtils.createInputDataType(geom, idt);
            List<EObject> list2 = new ArrayList<>();
            list2.add(input2);
            exeRequest.addInput(idt.getIdentifier().getValue(), list2);          
            
            // set distance input
            idt = (InputDescriptionType) pdt.getDataInputs().getInput().get(1);
            DataType input = WPSUtils.createInputDataType(bufferAmnt, idt);
            List<EObject> list = new ArrayList<>();
            list.add(input);
            exeRequest.addInput(idt.getIdentifier().getValue(), list);
        }
    }	
    @SuppressWarnings("rawtypes")
	public void runExecuteProcessBuffer_Manually(Geometry geom) throws ServiceException, IOException, ParseException {

        WPSCapabilitiesType capabilities = wps.getCapabilities();
        // get the first process and execute it
        ProcessOfferingsType processOfferings = capabilities.getProcessOfferings();
        EList processes = processOfferings.getProcess();
        // ProcessBriefType process = (ProcessBriefType) processes.get(0);

        // does the server contain the specific process I want
        boolean found = false;
        Iterator iterator = processes.iterator();
        while (iterator.hasNext()) {
            ProcessBriefType process = (ProcessBriefType) iterator.next();
            if (process.getIdentifier().getValue().equalsIgnoreCase(processIden)) {
                found = true;

                break;
            }
        }

        // exit test if my process doesn't exist on server
        if (!found) {
            return;
        }

        // do a full describe process on my process
        DescribeProcessRequest descRequest = wps.createDescribeProcessRequest();
        descRequest.setIdentifier(processIden);

        DescribeProcessResponse descResponse = wps.issueRequest(descRequest);

        // based on the describe process, setup the execute
        ProcessDescriptionsType processDesc = descResponse.getProcessDesc();
        ExecuteProcessRequest exeRequest = wps.createExecuteProcessRequest();
        exeRequest.setIdentifier(processIden);

        // set input data
        setLocalInputDataBufferPoly(exeRequest, processDesc, geom);

        // send the request
        ExecuteProcessResponse response = wps.issueRequest(exeRequest);

        // response should not be null and no exception should occur.
        assertNotNull(response);

        ExecuteResponseType executeResponse = response.getExecuteResponse();
        assertNotNull(executeResponse);

        ExceptionReportType exceptionResponse = response.getExceptionResponse();
        assertNull(exceptionResponse);

        // check that the result is expected
        Geometry expected = geom.buffer(350);
        EList outputs = executeResponse.getProcessOutputs().getOutput();
        OutputDataType output = (OutputDataType) outputs.get(0);
        Geometry result = (Geometry) output.getData().getComplexData().getData().get(0);
        // System.out.println(expected);
        // System.out.println(result);
        assertEquals(expected, result);
    }
	private void runExecuteProcessBuffer_Online(Geometry geom1) throws ServiceException, IOException, ParseException, ProcessException {

		WPSCapabilitiesType capabilities = wps.getCapabilities();

		// get the first process and execute it
		ProcessOfferingsType processOfferings = capabilities.getProcessOfferings();
		@SuppressWarnings("rawtypes")
		EList processes = processOfferings.getProcess();

		// does the server contain the specific process I want
		boolean found = false;
		@SuppressWarnings("rawtypes")
		Iterator iterator = processes.iterator();
		while (iterator.hasNext()) {
			ProcessBriefType process = (ProcessBriefType) iterator.next();
			if (process.getIdentifier().getValue().equalsIgnoreCase(processIden)) {
				found = true;
				break;
			}
		}
		// exit test if my process doesn't exist on server
		if (!found) {
			return;
		}

		// do a full describe process on my process
		DescribeProcessRequest descRequest = wps.createDescribeProcessRequest();
		descRequest.setIdentifier(processIden);

		ExecuteProcessRequest execRequest = wps.createExecuteProcessRequest();
		execRequest.setIdentifier(processIden);

		execRequest.addInput("distance", Arrays.asList(wps.createLiteralInputValue("350")));
		execRequest.addInput("geom", Arrays.asList(wps.createBoundingBoxInputValue("EPSG:4326", 2, Arrays.asList(-180.0, -90.0), Arrays.asList(-180.0, -90.0))));

		ResponseDocumentType respDoc = wps.createResponseDocumentType(false, true, true, "result");

		OutputDefinitionType rawOutput = wps.createOutputDefinitionType("test");

		ResponseFormType responseForm = wps.createResponseForm(respDoc, rawOutput);

		responseForm.setResponseDocument(respDoc);
		execRequest.setResponseForm(responseForm);
		execRequest.performPostOutput(System.out);

		
		DescribeProcessResponse descResponse = wps.issueRequest(descRequest);

		// based on the describe process, setup the execute
		ProcessDescriptionsType processDesc = descResponse.getProcessDesc();
		ProcessDescriptionType pdt = (ProcessDescriptionType) processDesc.getProcessDescription().get(0);
		
		WPSFactory wpsfactory = new WPSFactory(pdt, this.url);
		Process process = wpsfactory.create();

		// setup the inputs&
		Map<String, Object> map = new TreeMap<>();
		map.put("distance", 350);
		map.put("geom", geom1);

		// execute/send-request for the process
		Map<String, Object> results = process.execute(map, null);

		// check that the result is expected
		System.out.println("CHECK THE RESULTS:");
		assertNotNull(results);
		Geometry expected = geom1.buffer(350);
		Geometry result = (Geometry) results.get("result");
		assertNotNull(result);
		System.out.println("Expected : " + expected);
		System.out.println("Result : " + result);
		assertEquals(expected, result);
	}
	@SuppressWarnings("rawtypes")
	@Test
	@Ignore
	public void testGetWPSCapabilities() throws IOException, ServiceException, ParseException {

		WPSCapabilitiesType capabilities = wps.getCapabilities();

		// view a list of processes offered by the server
		ProcessOfferingsType processOfferings = capabilities.getProcessOfferings();
		EList processes = processOfferings.getProcess();
		Iterator iterator = processes.iterator();
		while (iterator.hasNext()) {
			ProcessBriefType process = (ProcessBriefType) iterator.next();
			System.out.println(process.getIdentifier().getValue());
		}

	}	
	@SuppressWarnings("rawtypes")
	@Test
    public void testExecute_2_Operands_Union() throws ServiceException, IOException, ParseException, ProcessException {

        String processIdenLocal = "JTS:union";
        WPSCapabilitiesType capabilities = wps.getCapabilities();

        // get the first process and execute it
        ProcessOfferingsType processOfferings = capabilities.getProcessOfferings();
        EList processes = processOfferings.getProcess();

        // does the server contain the specific process I want
        boolean found = false;
        Iterator iterator = processes.iterator();
        while (iterator.hasNext()) {
            ProcessBriefType process = (ProcessBriefType) iterator.next();
            if (process.getIdentifier().getValue().equalsIgnoreCase(processIdenLocal)) {
                found = true;
                break;
            }
        }
        // exit test if my process doesn't exist on server
        if (!found) {
            return;
        }

        // do a full describe process on my process
        DescribeProcessRequest descRequest = wps.createDescribeProcessRequest();
        descRequest.setIdentifier(processIdenLocal);

        DescribeProcessResponse descResponse = wps.issueRequest(descRequest);
        

        // based on the describe process, setup the execute
        ProcessDescriptionsType processDesc = descResponse.getProcessDesc();
        ProcessDescriptionType pdt = (ProcessDescriptionType) processDesc.getProcessDescription().get(0);
        System.out.println("INPUTS FOR UNION OPERATION:");
        for (int i = 0; i < pdt.getDataInputs().getInput().size(); i++ ) {
        	InputDescriptionType idt = (InputDescriptionType) pdt.getDataInputs().getInput().get(i);
        	System.out.println(idt.getIdentifier().getValue());
        }
        
        
        WPSFactory wpsfactory = new WPSFactory(pdt, this.url);
        Process process = wpsfactory.create();

        // setup the inputs
        Map<String, Object> map = new TreeMap<>();
        WKTReader reader = new WKTReader(new GeometryFactory());
        List<Geometry> list = new ArrayList<>();
        Geometry geom1 = reader.read("POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))");
        Geometry geom2 = reader.read("POLYGON((20 30, 30 0, 20 20, 80 20, 20 30))");
        Geometry geom3 = reader.read("POLYGON((177 10, 30 88, 40 70, 46 20, 177 10))");
        Geometry geom4 = reader.read("POLYGON((5 10, 5 0, 13 10, 5 20, 5 10))");
        list.add(geom1);
        list.add(geom2);
        list.add(geom3);
        list.add(geom4);
        map.put("geom", list);

        
        // execute/send-request for the process
        Map<String, Object> results = process.execute(map, null);
        // check that the result is expected
        assertNotNull(results);
//        Geometry expected = geom1.union(geom2);
//        expected = expected.union(geom3);
//        expected = expected.union(geom4);
        Geometry result = (Geometry) results.get("result");
        assertNotNull(result);
//        System.out.println(expected);
//        System.out.println(result);

    }
	

	@Test
	public void testExecute_1_Operand_Buffer_Online() throws IOException, ServiceException, ParseException {
		WKTReader reader = new WKTReader(new GeometryFactory());
		Geometry geom = reader.read("POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))");
//		Geometry geom2 = reader.read("POINT (160 200)");
//		Geometry geom3 = reader.read("LINESTRING (100 240, 220 140, 380 240, 480 220)");
//		Geometry geom4 = reader.read("MULTILINESTRING ((140 280, 180 180, 400 260), (340 120, 160 100, 80 200))");
//		Geometry geom5 = reader.read("MULTIPOINT (180 180, 260 280, 340 200)");
//		Geometry geom6 = reader.read("MULTIPOLYGON (((160 320, 120 140, 360 140, 320 340, 160 320), (440 260, 580 140, 580 240, 440 260)))");
		runExecuteProcessBuffer_Online(geom);
	}
	@Test
	public void testExecute_1_Operand_Buffer_Manually() throws IOException, ServiceException, ParseException {
		WKTReader reader = new WKTReader(new GeometryFactory());
		Geometry geom = reader.read("LINESTRING (100 240, 220 140, 380 240, 480 220)");
		runExecuteProcessBuffer_Manually(geom);

	}	
    @Test
    public void testGeoServer3CapabilitiesParsing() throws Exception {

        //URL url = new File("src/test/resources/wps/geoserverCapabilities.xml").toURI().toURL();
        URL url = new URL("http://localhost:8088/geoserver/ows?service=wps&version=1.0.0&request=GetCapabilities");

        Configuration config = new WPSConfiguration();
        Parser parser = new Parser(config);
        try (BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()))) {
            Object object = parser.parse(in);

            assertNotNull("parsed", object);

            WPSCapabilitiesType capabiliites = (WPSCapabilitiesType) object;
            assertEquals("1.0.0", capabiliites.getVersion());
        }
    }	
	@Test
    public void testExecute_1_Operand_Buffer_Make_Inputs_By_Call_Process() throws Exception {

    	// create a WebProcessingService as shown above, then do a full describeprocess on my process
    	DescribeProcessRequest descRequest = wps.createDescribeProcessRequest();
    	descRequest.setIdentifier(processIden); // describe the buffer process

    	// send the request and get the ProcessDescriptionType bean to create a WPSFactory
    	DescribeProcessResponse descResponse = wps.issueRequest(descRequest);
    	ProcessDescriptionsType processDesc = descResponse.getProcessDesc();
    	ProcessDescriptionType pdt = (ProcessDescriptionType) processDesc.getProcessDescription().get(0);
    	
        System.out.println("Print and fix INPUTS FOR BUFFER OPERATION:");
        for (int i = 0; i < pdt.getDataInputs().getInput().size(); i++ ) {
        	InputDescriptionType idt = (InputDescriptionType) pdt.getDataInputs().getInput().get(i);
        	if (idt.getLiteralData() != null)
        		if (idt.getLiteralData().getDataType() != null)
        			idt.getLiteralData().getDataType().setReference("3");
        		else {
        			DomainMetadataType dm = net.opengis.ows11.Ows11Factory.eINSTANCE.createDomainMetadataType();
        			idt.getLiteralData().setDataType(dm);
        			idt.getLiteralData().getDataType().setReference("3");
        		}
        			
        	System.out.println(idt.getIdentifier().getValue());
        }
        InputDescriptionType idt = (InputDescriptionType) pdt.getDataInputs().getInput().get(1);
        ValuesReferenceType v = Wps10Factory.eINSTANCE.createValuesReferenceType();
        v.setReference("350");
        idt.getLiteralData().setValuesReference(v);
        idt.getLiteralData().getDataType().setReference("350");    	
    	
    	WPSFactory wpsfactory = new WPSFactory(pdt, url);
    	

		Map<String, Object> map = new TreeMap<>();
		WKTReader reader = new WKTReader(new GeometryFactory());
		Geometry geom = reader.read("LINESTRING (100 240, 220 140, 380 240, 480 220)");
		map.put("distance", 350);
		map.put("geom", geom);

        Process process = wpsfactory.create();
        Map<String, Object> results = process.execute(map, null);
        assertNotNull(results);	
  }    

    @SuppressWarnings("rawtypes")
	@Test
    public void testExecute_1_Operand_Buffer_Centroid_Simplify_Make_Inputs_From_FeatureCollection() throws Exception {

    	String pIdentifier = "gs:Centroid";
    	// create a WebProcessingService as shown above, then do a full describeprocess on my process
    	DescribeProcessRequest descRequest = wps.createDescribeProcessRequest();
    	descRequest.setIdentifier(pIdentifier); // describe the buffer process

    	// send the request and get the ProcessDescriptionType bean to create a WPSFactory
    	DescribeProcessResponse descResponse = wps.issueRequest(descRequest);
    	ProcessDescriptionsType processDesc = descResponse.getProcessDesc();
    	ProcessDescriptionType pdt = (ProcessDescriptionType) processDesc.getProcessDescription().get(0);
    	
		Map<String, Object> map = new TreeMap<>();
		map.put("distance", 0.04);
		map.put("preserveTopology", true);
		
		String geojson = "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{\"name\":\"YUIGG\",\"geom_1\":null,\"geom_2\":null,\"id\":3256},\"geometry\":{\"type\":\"MultiPolygon\",\"coordinates\":[[[[-119.0635,37.571],[-119.5675,39.6261],[-117.5125,40.6148],[-116.4074,40.2271],[-119.0635,37.571]]]]}},{\"type\":\"Feature\",\"properties\":{\"name\":\"NNMJK\",\"geom_1\":null,\"geom_2\":null,\"id\":3257},\"geometry\":{\"type\":\"MultiPolygon\",\"coordinates\":[[[[-121.0603,37.0282],[-122.6501,38.5016],[-121.9328,38.7342],[-120.692,39.0444],[-120.6532,37.7067],[-118.8696,36.5823],[-120.1491,35.8456],[-121.0603,37.0282]]]]}},{\"type\":\"Feature\",\"properties\":{\"name\":\"HHJK5656\",\"geom_1\":null,\"geom_2\":null,\"id\":3252},\"geometry\":{\"type\":\"MultiPolygon\",\"coordinates\":[[[[-118.3268,43.077],[-116.5448,45.2139],[-116.1183,42.2671],[-119.3753,41.6467],[-120.8955,43.6877],[-118.3268,43.077]]]]}},{\"type\":\"Feature\",\"properties\":{\"name\":\"YUU09877\",\"geom_1\":null,\"geom_2\":null,\"id\":3255},\"geometry\":{\"type\":\"MultiPolygon\",\"coordinates\":[[[[-120.8955,45.7621],[-121.2154,41.6811],[-122.3399,41.7586],[-122.8052,43.0964],[-120.8955,45.7621]]]]}}]}";
        
		ExecuteProcessRequest execRequest = wps.createExecuteProcessRequest();
		execRequest.setIdentifier(pIdentifier);
		
        EList inputs = pdt.getDataInputs().getInput();
        Iterator iterator = inputs.iterator();
        while (iterator.hasNext()) {
            InputDescriptionType idt = (InputDescriptionType) iterator.next();
            String identifier = idt.getIdentifier().getValue();            
            List<EObject> list = new ArrayList<>();
        	if (idt.getIdentifier().getValue().equalsIgnoreCase("features")) {
        		DataType createdInput =  WPSUtils.createInputDataType(new CDATAEncoder(geojson), WPSUtils.INPUTTYPE_COMPLEXDATA, null, "application/json");
        		list.add(createdInput);
        		execRequest.addInput(identifier, list);
        	}else if (idt.getIdentifier().getValue().equalsIgnoreCase("distance")){
                // our value is a single object so create a single datatype for it
        		Object inputValue = map.get("distance");
                DataType createdInput = WPSUtils.createInputDataType(inputValue, idt);                        
                list.add(createdInput);                		
                execRequest.addInput(identifier, list);
        	}else if (idt.getIdentifier().getValue().equalsIgnoreCase("preserveTopology")){
        		Object inputValue = map.get("preserveTopology");
                DataType createdInput = WPSUtils.createInputDataType(inputValue, idt);                        
                list.add(createdInput);                		
                execRequest.addInput(identifier, list);        		
        	}
        }
        try {        	

            ExecuteProcessResponse response;
            response = wps.issueRequest(execRequest);            
            if ((response.getExceptionResponse() == null) && (response.getExecuteResponse() != null))
            {
                if (response.getExecuteResponse().getStatus().getProcessSucceeded() != null)
                {
                    for (Object processOutput : response.getExecuteResponse().getProcessOutputs().getOutput())
                    {
                        OutputDataType wpsOutput = (OutputDataType) processOutput;
                        FeatureCollectionType fc = (FeatureCollectionType) wpsOutput.getData().getComplexData().getData().get(0);
                        DefaultFeatureCollection dfs = (DefaultFeatureCollection) fc.getFeature().get(0);
                        Iterator<SimpleFeature> itr = dfs.iterator();
                        while (itr.hasNext()) {
                        	SimpleFeature sf = itr.next();
                            System.out.println(sf.getAttribute("id"));
                            System.out.println(sf.getDefaultGeometry().toString());
                        }

                    }
                }        	
            } else {     
            	System.out.println(response.getExceptionResponse().getException().get(0).toString());
            }

        } catch (Exception e) {
        	System.out.println(e.getMessage());
        }				
  }
    @Test
    public void testExecute_1_Operand_Buffer_Make_Inputs_From_GML() throws Exception {

		ExecuteProcessRequest execRequest = wps.createExecuteProcessRequest();
		execRequest.setIdentifier(processIden);
        try {
        	
            final URL finalURL = execRequest.getFinalURL();
            final HTTPResponse httpResponse;
            
            if (execRequest.requiresPost()) {
                final String postContentType = execRequest.getPostContentType();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                String input = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><wps:Execute version=\"1.0.0\" service=\"WPS\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"http://www.opengis.net/wps/1.0.0\" xmlns:wfs=\"http://www.opengis.net/wfs\" xmlns:wps=\"http://www.opengis.net/wps/1.0.0\" xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:gml=\"http://www.opengis.net/gml\" xmlns:ogc=\"http://www.opengis.net/ogc\" xmlns:wcs=\"http://www.opengis.net/wcs/1.1.1\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xsi:schemaLocation=\"http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd\">\r\n" + 
                		"  <ows:Identifier>geo:buffer</ows:Identifier>\r\n" + 
                		"  <wps:DataInputs>\r\n" + 
                		"    <wps:Input>\r\n" + 
                		"      <ows:Identifier>geom</ows:Identifier>\r\n" + 
                		"      <wps:Data>\r\n" + 
                		"        <wps:ComplexData mimeType=\"text/xml; subtype=gml/3.1.1\"><gml:MultiGeometry>\r\n" + 
                		"     <gml:Point>\r\n" + 
                		"        <gml:coordinates>400,600</gml:coordinates>\r\n" + 
                		"     </gml:Point>\r\n" + 
                		"     <gml:Point>\r\n" + 
                		"        <gml:coordinates>100,200</gml:coordinates>\r\n" + 
                		"     </gml:Point>\r\n" + 
                		"</gml:MultiGeometry></wps:ComplexData>\r\n" + 
                		"      </wps:Data>\r\n" + 
                		"    </wps:Input>\r\n" + 
                		"    <wps:Input>\r\n" + 
                		"      <ows:Identifier>distance</ows:Identifier>\r\n" + 
                		"      <wps:Data>\r\n" + 
                		"        <wps:LiteralData>50</wps:LiteralData>\r\n" + 
                		"      </wps:Data>\r\n" + 
                		"    </wps:Input>\r\n" + 
                		"  </wps:DataInputs>\r\n" + 
                		"</wps:Execute>";
                
                for (int i = 0; i < input.length(); ++i)
                	out.write(input.charAt(i));    
              	InputStream in = new ByteArrayInputStream(out.toByteArray()); 
              	httpClient = new SimpleHttpClient();
              	httpClient.setUser("thaihv");
              	httpClient.setPassword("123456789a");
				httpResponse = httpClient.post(finalURL, in, postContentType);
				
				ExecuteProcessResponse response = (ExecuteProcessResponse) execRequest.createResponse(httpResponse);
				if (response.getExecuteResponse() != null) {
					for (Object processOutput : response.getExecuteResponse().getProcessOutputs().getOutput()) {
						OutputDataType wpsOutput = (OutputDataType) processOutput;
						System.out.println(wpsOutput.getData().getComplexData().getData().get(0).toString());
					}					
				}
            } 

        } catch (Exception e) {
        	System.out.println(e.getMessage());
        }		
  }

	@Test
	public void testExecute_1_Operand_Area_getResults_LiteralData() throws Exception {
		String areaProcessIdent = "JTS:area";

		DataType createdInput_1 = WPSUtils.createInputDataType(new CDATAEncoder(
				"MULTIPOLYGON (((160 320, 120 140, 360 140, 320 340, 160 320), (440 260, 580 140, 580 240, 440 260)))"),
				WPSUtils.INPUTTYPE_COMPLEXDATA, null, "application/wkt");

		ExecuteProcessRequest exeRequest = wps.createExecuteProcessRequest();
		exeRequest.setIdentifier(areaProcessIdent);
		exeRequest.addInput("geom", Arrays.asList(createdInput_1));

		// send the request
		ExecuteProcessResponse response = wps.issueRequest(exeRequest);
		assertNotNull(response.getExecuteResponse().getProcessOutputs().getOutput().get(0));

		Object processOutput = response.getExecuteResponse().getProcessOutputs().getOutput().get(0);
		OutputDataType wpsOutput = (OutputDataType) processOutput;
		System.out.println(wpsOutput.getData().getLiteralData().getValue());
	}    
	
	@SuppressWarnings("unchecked")
	@Test
    public void testExecute_Call_Chaining_Process_AreaFromUnion() throws ServiceException, IOException, ParseException {

        // reference to the Cascaded Process
        EObject processUnionCascadeReference = Wps10Factory.eINSTANCE.createInputReferenceType();
        ((InputReferenceType) processUnionCascadeReference).setMimeType("application/xml");
        ((InputReferenceType) processUnionCascadeReference).setMethod(MethodType.POST_LITERAL);
        ((InputReferenceType) processUnionCascadeReference).setHref("http://geoserver/wps");

        ExecuteType processUnionExecType = Wps10Factory.eINSTANCE.createExecuteType();
        processUnionExecType.setVersion("1.0.0");
        processUnionExecType.setService("WPS");

        // ----
        String processIden = "JTS:union";

        CodeType resultsCodeType = Ows11Factory.eINSTANCE.createCodeType();
        resultsCodeType.setValue("result");

        CodeType codeType = Ows11Factory.eINSTANCE.createCodeType();
        codeType.setValue(processIden);

        processUnionExecType.setIdentifier(codeType);

        DataInputsType1 inputtypes = Wps10Factory.eINSTANCE.createDataInputsType1();
        processUnionExecType.setDataInputs(inputtypes);

        ResponseFormType processUnionResponseForm = Wps10Factory.eINSTANCE.createResponseFormType();
        OutputDefinitionType processUnionRawDataOutput = Wps10Factory.eINSTANCE.createOutputDefinitionType();
        processUnionRawDataOutput.setIdentifier(resultsCodeType);
        processUnionResponseForm.setRawDataOutput(processUnionRawDataOutput);

        processUnionExecType.setResponseForm(processUnionResponseForm);

        // set inputs
        // POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))
        ComplexDataType cdt1 = Wps10Factory.eINSTANCE.createComplexDataType();
        cdt1.getData().add(0, new CDATAEncoder("POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))"));
        cdt1.setMimeType("application/wkt");
        net.opengis.wps10.DataType data1 = Wps10Factory.eINSTANCE.createDataType();
        data1.setComplexData(cdt1);

        InputType input1 = Wps10Factory.eINSTANCE.createInputType();
        CodeType inputIdent = Ows11Factory.eINSTANCE.createCodeType();
        inputIdent.setValue("geom");
        input1.setIdentifier(inputIdent);
        input1.setData(data1);

        processUnionExecType.getDataInputs().getInput().add(input1);

        // POLYGON((2 1, 3 0, 4 1, 3 2, 2 1))
        ComplexDataType cdt2 = Wps10Factory.eINSTANCE.createComplexDataType();
        cdt2.getData().add(0, new CDATAEncoder("POLYGON((2 1, 3 0, 4 1, 3 2, 2 1))"));
        cdt2.setMimeType("application/wkt");
        net.opengis.wps10.DataType data2 = Wps10Factory.eINSTANCE.createDataType();
        data2.setComplexData(cdt2);

        InputType input2 = Wps10Factory.eINSTANCE.createInputType();
        CodeType inputIdent2 = Ows11Factory.eINSTANCE.createCodeType();
        inputIdent2.setValue("geom");
        input2.setIdentifier(inputIdent2);
        input2.setData(data2);

        processUnionExecType.getDataInputs().getInput().add(input2);

        // set the Cascade WPS process Body
        ((InputReferenceType) processUnionCascadeReference).setBody(new WPSEncodeDelegate(processUnionExecType, WPS.Execute));
        // ----
        String areaProcessIdent = "JTS:area";

        ExecuteProcessRequest exeRequest = wps.createExecuteProcessRequest();
        exeRequest.setIdentifier(areaProcessIdent);
        exeRequest.addInput("geom", Arrays.asList(processUnionCascadeReference));

        // send the request
        ExecuteProcessResponse response = wps.issueRequest(exeRequest);
        assertNotNull(response.getExecuteResponse().getProcessOutputs().getOutput().get(0));
        
		Object processOutput = response.getExecuteResponse().getProcessOutputs().getOutput().get(0);
		OutputDataType wpsOutput = (OutputDataType) processOutput;
		System.out.println(wpsOutput.getData().getLiteralData().getValue());
		
		
    }	
    class WPSEncodeDelegate implements EncoderDelegate {

        private Object value;

        private QName qname;

        public WPSEncodeDelegate(Object value, QName qname) {
            this.value = value;
            this.qname = qname;
        }

        @Override
        public void encode(ContentHandler output) throws Exception {
            WPSConfiguration config = new WPSConfiguration();
            Encoder encoder = new Encoder(config);
            encoder.encode(value, qname, output);
        }
    }
    class CDATAEncoder implements EncoderDelegate {
        String cData;

        public CDATAEncoder(String cData) {
            this.cData = cData;
        }

        @Override
        public void encode(ContentHandler output) throws Exception {
            ((LexicalHandler) output).startCDATA();
            Reader r = new StringReader(cData);
            char[] buffer = new char[1024];
            int read;
            while ((read = r.read(buffer)) > 0) {
                output.characters(buffer, 0, read);
            }
            r.close();
            ((LexicalHandler) output).endCDATA();
        }
    }    
}
