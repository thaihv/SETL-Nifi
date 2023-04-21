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
package com.jdvn.setl.geos.processors.db;

import org.apache.nifi.util.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class PostgreSQLDatabaseAdapter extends GenericDatabaseAdapter {
    @Override
    public String getName() {
        return "PostgreSQL";
    }

    @Override
    public String getDescription() {
        return "Generates PostgreSQL compatible SQL";
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    @Override
    public boolean supportsInsertIgnore() {
        return true;
    }

    @Override
    public String getUpsertStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        String columns = columnNames.stream()
            .collect(Collectors.joining(", "));

        String parameterizedInsertValues = columnNames.stream()
            .map(__ -> "?")
            .collect(Collectors.joining(", "));

        String updateValues = columnNames.stream()
            .map(columnName -> "EXCLUDED." + columnName)
            .collect(Collectors.joining(", "));

        String conflictClause = "(" + uniqueKeyColumnNames.stream().collect(Collectors.joining(", ")) + ")";

        StringBuilder statementStringBuilder = new StringBuilder("INSERT INTO ")
            .append(table)
            .append("(").append(columns).append(")")
            .append(" VALUES ")
            .append("(").append(parameterizedInsertValues).append(")")
            .append(" ON CONFLICT ")
            .append(conflictClause)
            .append(" DO UPDATE SET ")
            .append("(").append(columns).append(")")
            .append(" = ")
            .append("(").append(updateValues).append(")");

        return statementStringBuilder.toString();
    }

    @Override
    public String getInsertIgnoreStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        String columns = columnNames.stream()
                .collect(Collectors.joining(", "));

        String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        String conflictClause = "(" + uniqueKeyColumnNames.stream().collect(Collectors.joining(", ")) + ")";

        StringBuilder statementStringBuilder = new StringBuilder("INSERT INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedInsertValues).append(")")
                .append(" ON CONFLICT ")
                .append(conflictClause)
                .append(" DO NOTHING");
        return statementStringBuilder.toString();
    }


}
