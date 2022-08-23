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

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import com.cci.gss.jdbc.driver.IGSSConnection;
import com.cci.gss.jdbc.driver.IGSSDatabaseMetaData;
import com.cci.gss.jdbc.driver.IGSSStatement;

@Tags({ "GSS Store", "jdbc", "database", "connection", "pooling", "spatial data"})
@CapabilityDescription("GSS Store ControllerService implementation of GSSService.")
@DynamicProperties({
    @DynamicProperty(name = "JDBC property name",
            value = "JDBC property value",
            expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
            description = "JDBC driver property name and value applied to JDBC connections."),
    @DynamicProperty(name = "SENSITIVE.JDBC property name",
            value = "JDBC property value",
            expressionLanguageScope = ExpressionLanguageScope.NONE,
            description = "JDBC driver property name prefixed with 'SENSITIVE.' handled as a sensitive property.")
})
@RequiresInstanceClassLoading
public class GSSStore extends AbstractControllerService implements GSSService {

    /** Property Name Prefix for Sensitive Dynamic Properties */
    protected static final String SENSITIVE_PROPERTY_PREFIX = "SENSITIVE.";

    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MIN_IDLE} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_MIN_IDLE = "0";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MAX_IDLE} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_MAX_IDLE = "8";
    /**
     * Copied from private variable {@link BasicDataSource.maxConnLifetimeMillis} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_EVICTION_RUN_PERIOD = String.valueOf(-1L);
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.7.0
     * and converted from 1800000L to "1800000 millis" to "30 mins"
     */
    private static final String DEFAULT_MIN_EVICTABLE_IDLE_TIME = "30 mins";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME = String.valueOf(-1L);

    public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
        .name("Database Connection URL")
        .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters."
            + " The exact syntax of a database connection URL is specified by your DBMS.")
        .defaultValue(null)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
        .name("Database User")
        .description("Database user name")
        .defaultValue(null)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
        .name("Password")
        .description("The password for the database user")
        .defaultValue(null)
        .required(false)
        .sensitive(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
        .name("Max Wait Time")
        .description("The maximum amount of time that the pool will wait (when there are no available connections) "
            + " for a connection to be returned before failing, or -1 to wait indefinitely. ")
        .defaultValue("500 millis")
        .required(true)
        .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .sensitive(false)
        .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
        .name("Max Total Connections")
        .description("The maximum number of active connections that can be allocated from this pool at the same time, "
            + " or negative for no limit.")
        .defaultValue("64")
        .required(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .sensitive(false)
        .build();

    public static final PropertyDescriptor VALIDATION_QUERY = new PropertyDescriptor.Builder()
        .name("Validation-query")
        .displayName("Validation query")
        .description("Validation query used to validate connections before returning them. "
            + "When connection is invalid, it get's dropped and new valid connection will be returned. "
            + "Note!! Using validation might have some performance penalty.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
            .displayName("Minimum Idle Connections")
            .name("dbcp-min-idle-conns")
            .description("The minimum number of connections that can remain idle in the pool, without extra ones being " +
                    "created, or zero to create none.")
            .defaultValue(DEFAULT_MIN_IDLE)
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_IDLE = new PropertyDescriptor.Builder()
            .displayName("Max Idle Connections")
            .name("dbcp-max-idle-conns")
            .description("The maximum number of connections that can remain idle in the pool, without extra ones being " +
                    "released, or negative for no limit.")
            .defaultValue(DEFAULT_MAX_IDLE)
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
            .displayName("Max Connection Lifetime")
            .name("dbcp-max-conn-lifetime")
            .description("The maximum lifetime in milliseconds of a connection. After this time is exceeded the " +
                    "connection will fail the next activation, passivation or validation test. A value of zero or less " +
                    "means the connection has an infinite lifetime.")
            .defaultValue(DEFAULT_MAX_CONN_LIFETIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor EVICTION_RUN_PERIOD = new PropertyDescriptor.Builder()
            .displayName("Time Between Eviction Runs")
            .name("dbcp-time-between-eviction-runs")
            .description("The number of milliseconds to sleep between runs of the idle connection evictor thread. When " +
                    "non-positive, no idle connection evictor thread will be run.")
            .defaultValue(DEFAULT_EVICTION_RUN_PERIOD)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .displayName("Minimum Evictable Idle Time")
            .name("dbcp-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for eviction.")
            .defaultValue(DEFAULT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SOFT_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .displayName("Soft Minimum Evictable Idle Time")
            .name("dbcp-soft-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for " +
                    "eviction by the idle connection evictor, with the extra condition that at least a minimum number of" +
                    " idle connections remain in the pool. When the not-soft version of this option is set to a positive" +
                    " value, it is examined first by the idle connection evictor: when idle connections are visited by " +
                    "the evictor, idle time is first compared against it (without considering the number of idle " +
                    "connections in the pool) and then against this soft option, including the minimum idle connections " +
                    "constraint.")
            .defaultValue(DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(VALIDATION_QUERY);
        props.add(MIN_IDLE);
        props.add(MAX_IDLE);
        props.add(MAX_CONN_LIFETIME);
        props.add(EVICTION_RUN_PERIOD);
        props.add(MIN_EVICTABLE_IDLE_TIME);
        props.add(SOFT_MIN_EVICTABLE_IDLE_TIME);

        properties = Collections.unmodifiableList(props);
    }

    private volatile BasicDataSource dataSource;

    private DbmsType dbmsType;
	private static final Set<String> EXCLUSION_TABLES = new HashSet<String>(Arrays.asList(new String[] {
			"DUMMY4TEMP", "GEOMETRY_COLUMNS", "NETWORKS", "NETWORK_RELATIONS",
			"PATHREGISTRY", "PATHS", "PYRAMID_INFO", "RELREGISTRY", "RASTER_INFO",
			"SPATIAL_REF_SYS", "THEMES", "UPDATE_HISTORY", "GEOMETRY_LODS"
		}));    

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR);

        if (propertyDescriptorName.startsWith(SENSITIVE_PROPERTY_PREFIX)) {
            builder.sensitive(true).expressionLanguageSupported(ExpressionLanguageScope.NONE);
        } else {
            builder.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY);
        }

        return builder.build();
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        final String driverName = "com.cci.gss.driver.GSSDriver";
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        final String dburl = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        final Long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions());
        final Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        final Integer maxIdle = context.getProperty(MAX_IDLE).evaluateAttributeExpressions().asInteger();
        final Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        final Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD).evaluateAttributeExpressions());
        final Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        final Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());


        dataSource = new BasicDataSource();
        dataSource.setDriver(getDriver(driverName, dburl));
        dataSource.setMaxWaitMillis(maxWaitMillis);
        dataSource.setMaxTotal(maxTotal);
        dataSource.setMinIdle(minIdle);
        dataSource.setMaxIdle(maxIdle);
        dataSource.setMaxConnLifetimeMillis(maxConnLifetimeMillis);
        dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        dataSource.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);

        if (validationQuery!=null && !validationQuery.isEmpty()) {
            dataSource.setValidationQuery(validationQuery);
            dataSource.setTestOnBorrow(true);
        }

        dataSource.setUrl(dburl);
        dataSource.setUsername(user);
        dataSource.setPassword(passw);

        final List<PropertyDescriptor> dynamicProperties = context.getProperties()
                .keySet()
                .stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toList());

        dynamicProperties.forEach((descriptor) -> {
            final PropertyValue propertyValue = context.getProperty(descriptor);
            if (descriptor.isSensitive()) {
                final String propertyName = StringUtils.substringAfter(descriptor.getName(), SENSITIVE_PROPERTY_PREFIX);
                dataSource.addConnectionProperty(propertyName, propertyValue.getValue());
            } else {
                dataSource.addConnectionProperty(descriptor.getName(), propertyValue.evaluateAttributeExpressions().getValue());
            }
        });
    }

    private Driver getDriver(final String driverName, final String url) {
        final Class<?> clazz;

        try {
            clazz = Class.forName(driverName);
        } catch (final ClassNotFoundException e) {
            throw new ProcessException("Driver class " + driverName +  " is not found", e);
        }

        try {
            return DriverManager.getDriver(url);
        } catch (final SQLException e) {
            // In case the driver is not registered by the implementation, we explicitly try to register it.
            try {
                final Driver driver = (Driver) clazz.newInstance();
                DriverManager.registerDriver(driver);
                return DriverManager.getDriver(url);
            } catch (final SQLException e2) {
                throw new ProcessException("No suitable driver for the given Database Connection URL", e2);
            } catch (final IllegalAccessException | InstantiationException e2) {
                throw new ProcessException("Creating driver instance is failed", e2);
            }
        }
    }

    private Long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? -1 : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }

    @OnDisabled
    public void shutdown() throws SQLException {
        try {
            if (dataSource != null) {
                dataSource.close();
            }
        } finally {
            dataSource = null;
        }
    }

    @Override
    public IGSSConnection getConnection() throws ProcessException {
        try {
            final IGSSConnection con;
            
            con = dataSource.getConnection().unwrap(IGSSConnection.class);
            
    		try {
    			String dbmsTypeString = con.getProperty(PropertyConstants.GSS_DBMS_TYPE);
    			if (dbmsTypeString != null) {
    				dbmsType = DbmsType.valueOf(dbmsTypeString);
    			}
    		}
    		catch (Throwable t) {
    		}
    		
    		if (dbmsType == null) {
    			System.err.println("The type of backend dbms can't be identified. It will be considered as an ORACLE.");
    			dbmsType = DbmsType.oracle;
    		}            
            
            return con;
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    
    @Override
    public String toString() {
        return "GSSStoreService[id=" + getIdentifier() + "]";
    }

    BasicDataSource getDataSource() {
        return dataSource;
    }
    
	public DbmsType getBackendDBMSType() {
		return dbmsType;
	}
	
    @Override
    public boolean isView(String dataName) throws SQLException {
		IGSSConnection conn = getConnection();
		IGSSStatement stmt = null;

		stmt = conn.createStatement();
		
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT COUNT(*) FROM ALL_VIEWS WHERE VIEW_NAME='");
		sb.append(dataName).append("' AND OWNER=USER");
		
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(sb.toString());
			rs.next();
			return rs.getInt(1) > 0;
		}
		finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}
	@Override
	public String[] getAllFeatureTableNames() {

		IGSSConnection conn = getConnection();
		IGSSStatement stmt = null;

		List<String> dataNames = new ArrayList<String>();
		
		try {
			stmt = conn.createStatement();
			for (String name : stmt.getAllLayerNames()) {
				int indexOfDot = name.lastIndexOf('.');
				if (indexOfDot != -1) {
					name = name.substring(indexOfDot + 1);
				}

				dataNames.add(name);
			}

			Collections.sort(dataNames);
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return dataNames.toArray(new String[dataNames.size()]);
	}
	@Override
	public String[] getAllDataNames() throws SQLException {

		IGSSConnection conn = getConnection();
		IGSSStatement stmt = null;
		ResultSet rs = null;
		IGSSDatabaseMetaData databaseMetadata = null;

		stmt = conn.createStatement();

		List<String> dataNames = new ArrayList<String>();
		for (String name : stmt.getAllLayerNames()) {
			int indexOfDot = name.lastIndexOf('.');
			if (indexOfDot != -1) {
				name = name.substring(indexOfDot + 1);
			}

			dataNames.add(name);
		}

		Collections.sort(dataNames);

		String catalog = null;
		String scheme = null;
		String dbmsPrefix = null;
		if (DbmsType.mssql == dbmsType) {
			rs = stmt.executeNativeQuery("SELECT UPPER(DB_NAME())");
			rs.next();
			catalog = rs.getString(1);
			rs.close();

			rs = stmt.executeNativeQuery("SELECT SCHEMA_NAME()");
			rs.next();
			scheme = rs.getString(1);
			rs.close();

			dbmsPrefix = catalog + "." + scheme;
		} else if (DbmsType.derby == dbmsType) {
			dbmsPrefix = scheme = "APP";
		} else {
			rs = stmt.executeNativeQuery("SELECT USER FROM DUAL");
			rs.next();
			scheme = rs.getString(1);
			rs.close();

			dbmsPrefix = scheme;
		}

		List<String> tabNames = new ArrayList<String>();

		boolean hasGeometryLods = false;
		databaseMetadata = (IGSSDatabaseMetaData) conn.getMetaData();
		rs = databaseMetadata.getTables(catalog, scheme, "%", new String[] { "TABLE", "VIEW" });
		while (rs.next()) {
			String name = rs.getString("TABLE_NAME").toUpperCase();
			if (name.equals("GEOMETRY_LODS")) {
				hasGeometryLods = true;
			}

			if (name.startsWith("BIN$") || name.startsWith("CREATE$JAVA$LOB$TABLE") || name.startsWith("MDRT_")
					|| EXCLUSION_TABLES.contains(name)) {
				continue;
			}

			tabNames.add(name);
		}
		rs.close();

		databaseMetadata.close();

		String operator = DbmsType.mssql == dbmsType ? "+" : "||";
		String prefix = DbmsType.mssql == dbmsType ? "GSS.dbo" : (DbmsType.derby == dbmsType ? "APP" : "GSS");

		StringBuffer sb = new StringBuffer();
		sb.append("SELECT 'X' ").append(operator);
		if (DbmsType.derby == dbmsType) {
			sb.append(" CHAR(THEME_ID)");
		} else {
			sb.append(" CAST(THEME_ID as VARCHAR(50))");
		}
		sb.append(" FROM ").append(prefix).append(".THEMES WHERE OWNER='").append(dbmsPrefix).append("'");
		sb.append(" UNION ");
		sb.append("SELECT THEME_NAME FROM ").append(prefix).append(".THEMES WHERE OWNER='").append(dbmsPrefix)
				.append("'");
		sb.append(" UNION ");
		sb.append("SELECT G_TABLE_NAME FROM ").append(prefix).append(".GEOMETRY_COLUMNS WHERE G_TABLE_CATALOG='")
				.append((catalog == null ? "DEFAULT" : catalog)).append("' AND G_TABLE_SCHEMA='").append(scheme)
				.append("'");
		sb.append(" UNION ");
		sb.append("SELECT NODE_TABLE_NAME FROM ").append(prefix).append(".NETWORKS WHERE OWNER='").append(dbmsPrefix)
				.append("'");
		sb.append(" UNION ");
		sb.append("SELECT LINK_TABLE_NAME FROM ").append(prefix).append(".NETWORKS WHERE OWNER='").append(dbmsPrefix)
				.append("'");
		sb.append(" UNION ");
		sb.append("SELECT 'XL' ").append(operator);
		if (DbmsType.derby == dbmsType) {
			sb.append(" CHAR(NETWORK_ID)");
		} else {
			sb.append(" CAST(NETWORK_ID AS VARCHAR(50))");
		}
		sb.append(" FROM ").append(prefix).append(".NETWORKS WHERE OWNER='").append(dbmsPrefix).append("'");
		sb.append(" UNION ");
		sb.append("SELECT 'XN' ").append(operator);
		if (DbmsType.derby == dbmsType) {
			sb.append(" CHAR(NETWORK_ID)");
		} else {
			sb.append(" CAST(NETWORK_ID AS VARCHAR(50))");
		}
		sb.append(" FROM ").append(prefix).append(".NETWORKS WHERE OWNER='").append(dbmsPrefix).append("'");

		if (hasGeometryLods) {
			sb.append(" UNION ");
			sb.append("SELECT 'L' ").append(operator);
			if (DbmsType.derby == dbmsType) {
				sb.append(" CHAR(THEME_ID)");
			} else {
				sb.append(" CAST(THEME_ID as VARCHAR(50))");
			}
			sb.append(" FROM ").append(prefix).append(".GEOMETRY_LODS");
		}

		rs = stmt.executeNativeQuery(sb.toString());
		while (rs.next()) {
			tabNames.remove(rs.getString(1));
		}
		rs.close();

		if (conn.getGSSVersion().compareTo(3, 5, 0) >= 0) {
			sb.setLength(0);
			sb.append("SELECT RASTER_NAME FROM ").append(prefix).append(".RASTER_INFO");
			try {
				rs = stmt.executeNativeQuery(sb.toString());
				while (rs.next()) {
					tabNames.remove(rs.getString("RASTER_NAME"));
				}
			} catch (SQLException e) {
			}
		}

		Collections.sort(tabNames);

		dataNames.addAll(tabNames);

		rs.close();
		stmt.close();
		if (databaseMetadata != null) {
			databaseMetadata.close();
		}
		conn.close();
		return dataNames.toArray(new String[dataNames.size()]);
	}


}
