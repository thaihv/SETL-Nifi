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

import java.sql.SQLException;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import com.cci.gss.jdbc.driver.IGSSConnection;
import com.cci.gss.jdbc.driver.IGSSPreparedStatement;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;


public class TestGSSGeometry {
	private static final String SERVICE_ID = GSSStore.class.getName();
    @Before
    public void init() {

    }
    @Test
    public void updateGeometry() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final GSSStore service = new GSSStore();

        runner.addControllerService(SERVICE_ID, service);
        final String url = "jdbc:gss://103.82.21.229:8844";
        runner.setProperty(service, GSSStore.DATABASE_URL, url);
        runner.setProperty(service, GSSStore.DB_USER, "NIFI2_TEST");
        runner.setProperty(service, GSSStore.DB_PASSWORD, "NIFI2_TEST");
        runner.enableControllerService(service);
        
        IGSSConnection conn = service.getConnection();
		try {

			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append("UPDATE CONSTR_PERMIT_TG SET SHAPE = GEOMFROMWKB(?) WHERE NIFIUID = ?");
			final IGSSPreparedStatement stmt = (IGSSPreparedStatement) conn.prepareStatement(sqlBuilder.toString());
						
			String wkt = "POLYGON ((11428412.79185542 1139824.629016196, 11434973.885236919 1157938.1229933489, 11501933.094139466 1153372.0213288753, 11463438.280422324 1135070.6969142132, 11428412.79185542 1139824.629016196))";
			WKTReader reader = new WKTReader();
			Geometry g = null;
			try {
				g = reader.read(wkt);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			byte[] wkb = new WKBWriter().write(g);
			stmt.setBytes(1, wkb);
			
			
			String nifiid = "9fbadc02-1d28-3711-9048-1df81a0bc543";
			stmt.setObject(2, nifiid);
			
			stmt.executeUpdate();
			
		} catch (SQLException e) {

			e.printStackTrace();
		}
        conn.close();
                
    }  
}
