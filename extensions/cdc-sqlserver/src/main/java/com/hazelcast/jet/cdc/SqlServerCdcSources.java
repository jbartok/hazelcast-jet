/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc;

import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.impl.AbstractSourceBuilder;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.pipeline.StreamSource;

/**
 * Contains factory methods for creating change data capture sources
 *
 * @since 4.1
 */
@EvolvingApi
public final class SqlServerCdcSources {

    private SqlServerCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from an SQL Server
     * database to Hazelcast Jet.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also
     * to construct the source once configuration is done
     */
    public static Builder sqlserver(String name) {
        return new Builder(name);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a SQL Server database to Hazelcast Jet.
     */
    public static final class Builder extends AbstractSourceBuilder<Builder> {

        private static final PropertyRules RULES = new PropertyRules()
                .mandatory("database.hostname")
                .mandatory("database.user")
                .mandatory("database.password")
                .mandatory("database.dbname")
                .mandatory("database.server.name")
                .exclusive("table.whitelist", "table.blacklist");

        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        private Builder(String name) {
            super(name, "io.debezium.connector.sqlserver.SqlServerConnector");
        }

        /**
         * IP address or hostname of the database server, has to be
         * specified.
         */
        public Builder setDatabaseAddress(String address) {
            return setProperty("database.hostname", address);
        }

        /**
         * Optional port number of the database server, if unspecified
         * defaults to the database specific default port (1433).
         */
        public Builder setDatabasePort(int port) {
            return setProperty("database.port", Integer.toString(port));
        }

        /**
         * Database user for connecting to the database server. Has to
         * be specified.
         */
        public Builder setDatabaseUser(String user) {
            return setProperty("database.user", user);
        }

        /**
         * Database user password for connecting to the database server.
         * Has to be specified.
         */
        public Builder setDatabasePassword(String password) {
            return setProperty("database.password", password);
        }

        /**
         * The name of the SQL Server database from which to stream the
         * changes. Has to be set.
         */
        public Builder setDatabaseName(String dbName) {
            return setProperty("database.dbname", dbName);
        }

        /**
         * Logical name that identifies and provides a namespace for the
         * particular database server/cluster being monitored. The
         * logical name should be unique across all other connectors.
         * Only alphanumeric characters and underscores should be used.
         * Has to be specified.
         */
        public Builder setClusterName(String cluster) {
            return setProperty("database.server.name", cluster);
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be monitored; any table not
         * included in the whitelist will be excluded from monitoring.
         * Each identifier is of the form <i>schemaName.tableName</i>.
         * By default the connector will monitor every non-system table
         * in each monitored database. May not be used with
         * {@link #setTableBlacklist(String...) table blacklist}.
         */
        public Builder setTableWhitelist(String... tableNameRegExps) {
            return setProperty("table.whitelist", tableNameRegExps);
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be excluded from monitoring; any
         * table not included in the blacklist will be monitored. Each
         * identifier is of the form <i>schemaName.tableName</i>. May
         * not be used with
         * {@link #setTableWhitelist(String...) table whitelist}.
         */
        public Builder setTableBlacklist(String... tableNameRegExps) {
            return setProperty("table.blacklist", tableNameRegExps);
        }

        /**
         * Optional regular expressions that match the fully-qualified
         * names of columns that should be excluded from change event
         * message values. Fully-qualified names for columns are of the
         * form <i>schemaName.tableName.columnName</i>. Note that
         * primary key columns are always included in the event’s key,
         * even if blacklisted from the value.
         */
        public Builder setColumnBlacklist(String... columnNameRegExps) {
            return setProperty("column.blacklist", columnNameRegExps);
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        public StreamSource<ChangeEvent> build() {
            RULES.check(properties);
            return connect(properties);
        }

    }
}
