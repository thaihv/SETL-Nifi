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
package com.jdvn.setl.geos.gss;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import com.cci.gss.jdbc.driver.IGSSConnection;
import com.cci.gss.jdbc.driver.IGSSStatement;

public class TestGSSStoreService {
	private static final String SERVICE_ID = GSSStoreService.class.getName();
    @Before
    public void init() {

    }
    @Test
    public void getConnectGSSStore() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final GSSStoreService service = new GSSStoreService();

        runner.addControllerService(SERVICE_ID, service);
        final String url = "jdbc:gss://14.160.24.128:8844";
        runner.setProperty(service, GSSStoreService.DATABASE_URL, url);
        runner.setProperty(service, GSSStoreService.DB_USER, "LO_VN2");
        runner.setProperty(service, GSSStoreService.DB_PASSWORD, "LO_VN2");
        runner.setProperty(service, GSSStoreService.DB_DRIVERNAME, "com.cci.gss.driver.GSSDriver");
        runner.enableControllerService(service);
        
        IGSSConnection conn = service.getConnection();
        System.out.println(conn.getProperty(PropertyConstants.GSS_DBMS_TYPE));
        conn.close();
		for (String name : service.getAllFeatureTableNames()) {
			System.out.println(name);
		}
		for (String name : service.getAllDataNames()) {
			System.out.println(name);
		}		
//		for (String name : service.getAllDataNames()) {
//			if (service.isLayer(name))
//				System.out.println("Layer: " + name);
//			else 
//				if (service.isView(name)) {
//					System.out.println("View: " + name);
//				} 
//				else
//					System.out.println(name);
//		}
    }
    @Test
    public void setGSSService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final GSSStoreService service = new GSSStoreService();

        runner.addControllerService(SERVICE_ID, service);
        final String url = "jdbc:gss://14.160.24.128:8844";
        runner.setProperty(service, GSSStoreService.DATABASE_URL, url);
        runner.setProperty(service, GSSStoreService.DB_USER, "LO_VN2");
        runner.setProperty(service, GSSStoreService.DB_PASSWORD, "LO_VN2");
        runner.setProperty(service, GSSStoreService.DB_DRIVERNAME, "com.cci.gss.driver.GSSDriver");
        runner.enableControllerService(service);
        runner.assertValid(service);
    }    
    

}
