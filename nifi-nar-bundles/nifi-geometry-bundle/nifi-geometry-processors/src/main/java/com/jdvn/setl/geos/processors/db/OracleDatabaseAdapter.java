
package com.jdvn.setl.geos.processors.db;

import org.apache.commons.lang3.StringUtils;

public class OracleDatabaseAdapter implements DatabaseAdapter {
    @Override
    public String getName() {
        return "Oracle";
    }

    @Override
    public String getDescription() {
        return "Generates Oracle compliant SQL";
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset) {
        return getSelectStatement(tableName, columnNames, whereClause, orderByClause, limit, offset, null);
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset, String columnForPartitioning) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        final StringBuilder query = new StringBuilder();
        boolean nestedSelect = (limit != null || offset != null) && StringUtils.isEmpty(columnForPartitioning);
        if (nestedSelect) {
            // Need a nested SELECT query here in order to use ROWNUM to limit the results
            query.append("SELECT ");
            if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
                query.append("*");
            } else {
                query.append(columnNames);
            }
            query.append(" FROM (SELECT a.*, ROWNUM rnum FROM (");
        }

        query.append("SELECT ");
        if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
            query.append("*");
        } else {
            query.append(columnNames);
        }
        query.append(" FROM ");
        query.append(tableName);

        if (!StringUtils.isEmpty(whereClause)) {
            query.append(" WHERE ");
            query.append(whereClause);
            if (!StringUtils.isEmpty(columnForPartitioning)) {
                query.append(" AND ");
                query.append(columnForPartitioning);
                query.append(" >= ");
                query.append(offset != null ? offset : "0");
                if (limit != null) {
                    query.append(" AND ");
                    query.append(columnForPartitioning);
                    query.append(" < ");
                    query.append((offset == null ? 0 : offset) + limit);
                }
            }
        }
        if (!StringUtils.isEmpty(orderByClause) && StringUtils.isEmpty(columnForPartitioning)) {
            query.append(" ORDER BY ");
            query.append(orderByClause);
        }
        if (nestedSelect) {
            query.append(") a");
            long offsetVal = 0;
            if (offset != null) {
                offsetVal = offset;
            }
            if (limit != null) {
                query.append(" WHERE ROWNUM <= ");
                query.append(offsetVal + limit);
            }
            query.append(") WHERE rnum > ");
            query.append(offsetVal);
        }

        return query.toString();
    }

    @Override
    public String getTableAliasClause(String tableName) {
        return tableName;
    }
}
