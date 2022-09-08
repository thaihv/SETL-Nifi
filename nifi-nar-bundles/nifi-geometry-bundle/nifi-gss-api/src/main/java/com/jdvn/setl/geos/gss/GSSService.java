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

import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.REJECT_AND_TERMINATE;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult;
import org.apache.nifi.processor.exception.ProcessException;

import com.cci.gss.jdbc.driver.IGSSConnection;

@Tags({ "gss", "geo spatial server", "database" })
@CapabilityDescription("GSS Service API. Connections can be asked from pool and returned after usage.")
public interface GSSService extends ControllerService {
	IGSSConnection getConnection() throws ProcessException;

	public boolean hasTransaction(String txName);
	public void enableTransaction(boolean enable, String txName);
	public void commit(String txName);
	public void rollback(String txName);
	
	default IGSSConnection getConnection(String txName) throws ProcessException {
		return getConnection();
	}
	default void returnConnection(Connection connection) {
	}

	default boolean isWorkingWell() {
		return false;
	}

	default FlowFileFilter getFlowFileFilter() {
		return null;
	}

	default FlowFileFilter getFlowFileFilter(int batchSize) {
		final FlowFileFilter filter = getFlowFileFilter();
		if (filter == null) {
			return null;
		}

		final AtomicInteger count = new AtomicInteger(0);
		return flowFile -> {
			if (count.get() >= batchSize) {
				return REJECT_AND_TERMINATE;
			}

			final FlowFileFilterResult result = filter.filter(flowFile);
			if (ACCEPT_AND_CONTINUE.equals(result)) {
				count.incrementAndGet();
				return ACCEPT_AND_CONTINUE;
			} else {
				return result;
			}
		};
	}
}
