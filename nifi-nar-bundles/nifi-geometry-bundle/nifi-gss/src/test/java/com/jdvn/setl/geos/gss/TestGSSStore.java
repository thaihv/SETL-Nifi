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

import static org.junit.Assert.assertTrue;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import com.cci.gss.jdbc.driver.IBaseStatement;
import com.cci.gss.jdbc.driver.IGSSConnection;
import com.cci.gss.jdbc.driver.IGSSResultSet;
import com.cci.gss.jdbc.driver.IGSSResultSetMetaData;

public class TestGSSStore {
	private static final String SERVICE_ID = GSSStore.class.getName();
    @Before
    public void init() {

    }
    @Test
    public void getConnectGSSStore() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final GSSStore service = new GSSStore();

        runner.addControllerService(SERVICE_ID, service);
        final String url = "jdbc:gss://14.160.24.128:8844";
        runner.setProperty(service, GSSStore.DATABASE_URL, url);
        runner.setProperty(service, GSSStore.DB_USER, "LO_VN2");
        runner.setProperty(service, GSSStore.DB_PASSWORD, "LO_VN2");
        runner.enableControllerService(service);
        
        IGSSConnection conn = service.getConnection();
        System.out.println(conn.getProperty(PropertyConstants.GSS_DBMS_TYPE));
        System.out.println(conn.getCurrentDriverVersion());
        System.out.println(conn.getMetaData().getUserName());
        assertTrue(service.isWorkingWell());
        conn.close();
        
    }
    @Test
    public void setGSSService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final GSSStore service = new GSSStore();

        runner.addControllerService(SERVICE_ID, service);
        final String url = "jdbc:gss://14.160.24.128:8844";
        runner.setProperty(service, GSSStore.DATABASE_URL, url);
        runner.setProperty(service, GSSStore.DB_USER, "LO_VN2");
        runner.setProperty(service, GSSStore.DB_PASSWORD, "LO_VN2");
        runner.enableControllerService(service);
        runner.assertValid(service);
    }    
    
    @Test
    public void metadata() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final GSSStore service = new GSSStore();

        runner.addControllerService(SERVICE_ID, service);
        final String url = "jdbc:gss://14.160.24.128:8844";
        runner.setProperty(service, GSSStore.DATABASE_URL, url);
        runner.setProperty(service, GSSStore.DB_USER, "LO_VN2");
        runner.setProperty(service, GSSStore.DB_PASSWORD, "LO_VN2");
        runner.enableControllerService(service);
        
        IGSSConnection conn = service.getConnection();
		List<String> columns = new ArrayList<>();
		try {
			String layerName = "GEO_SRC";
			Statement stmt = conn.createStatement();
			IGSSResultSetMetaData md = ((IBaseStatement) stmt).querySchema(layerName);
			
			int n = md.getColumnCount();
			for (int i = 0; i < n; i++ ) {
				String fieldName = md.getColumnName(i+1).toUpperCase();
				if (!fieldName.equals(md.getGeometryColumn()))
					columns.add(fieldName);
			}			
		} catch (SQLException e) {

			e.printStackTrace();
		}

		System.out.println(columns);
        conn.close();
    } 
}
