
package com.jdvn.setl.geos.processors.WPStransform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.eclipse.emf.common.util.EList;
import org.geotools.data.wps.WPSFactory;
import org.geotools.data.wps.WebProcessingService;
import org.geotools.data.wps.request.DescribeProcessRequest;
import org.geotools.data.wps.request.ExecuteProcessRequest;
import org.geotools.data.wps.response.DescribeProcessResponse;
import org.geotools.ows.ServiceException;
import org.geotools.process.Process;
import org.geotools.process.ProcessException;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import net.opengis.wps10.OutputDefinitionType;
import net.opengis.wps10.ProcessBriefType;
import net.opengis.wps10.ProcessDescriptionType;
import net.opengis.wps10.ProcessDescriptionsType;
import net.opengis.wps10.ProcessOfferingsType;
import net.opengis.wps10.ResponseDocumentType;
import net.opengis.wps10.ResponseFormType;
import net.opengis.wps10.WPSCapabilitiesType;

public class WPStransformTest {
	private WebProcessingService wps;
	private URL url;
	private String processIden;

	protected String getFixtureId() {
		return "wps";
	}

	protected Properties createExampleFixture() {
		Properties example = new Properties();
		example.put("service", "http://localhost:8088/geoserver/ows?service=wps&version=1.0.0&request=GetCapabilities");
		example.put("processId", "JTS:buffer");
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
	private void runExecuteProcessBufferLocal(Geometry geom1)
			throws ServiceException, IOException, ParseException, ProcessException {

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

		execRequest.addInput("buffer", Arrays.asList(wps.createLiteralInputValue("350")));
		execRequest.addInput("geom1", Arrays.asList(wps.createBoundingBoxInputValue("EPSG:4326", 2, Arrays.asList(-180.0, -90.0), Arrays.asList(-180.0, -90.0))));

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

		// setup the inputs
		Map<String, Object> map = new TreeMap<>();
		map.put("buffer", 350);
		map.put("geom1", geom1);

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
	public void testGetWPSCapabilities() throws IOException, ServiceException, ParseException {

		WPSCapabilitiesType capabilities = wps.getCapabilities();

		// view a list of processes offered by the server
		ProcessOfferingsType processOfferings = capabilities.getProcessOfferings();
		EList processes = processOfferings.getProcess();
		Iterator iterator = processes.iterator();
		while (iterator.hasNext()) {
			ProcessBriefType process = (ProcessBriefType) iterator.next();
			//System.out.println(process.getIdentifier().getValue());
		}

		WKTReader reader = new WKTReader(new GeometryFactory());
		Geometry geom1 = reader.read("POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))");
		Geometry geom2 = reader.read("POINT (160 200)");
		Geometry geom3 = reader.read("LINESTRING (100 240, 220 140, 380 240, 480 220)");
		Geometry geom4 = reader.read("MULTILINESTRING ((140 280, 180 180, 400 260), (340 120, 160 100, 80 200))");
		Geometry geom5 = reader.read("MULTIPOINT (180 180, 260 280, 340 200)");
		Geometry geom6 = reader.read("MULTIPOLYGON (((160 320, 120 140, 360 140, 320 340, 160 320), (440 260, 580 140, 580 240, 440 260)))");

		// run the local buffer execute test for each geom input
		runExecuteProcessBufferLocal(geom1);
		runExecuteProcessBufferLocal(geom2);
		runExecuteProcessBufferLocal(geom3);
		runExecuteProcessBufferLocal(geom4);
		runExecuteProcessBufferLocal(geom5);
		runExecuteProcessBufferLocal(geom6);

	}

}
