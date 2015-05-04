/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import com.google.common.base.Optional;

import com.datastax.driver.core.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.schema.LegacySchemaTables;

public class NativeSSTableLoaderClient extends SSTableLoader.Client
{
    protected final Map<String, CFMetaData> tables;
    private final Collection<InetAddress> hosts;
    private final int port;
    private final String username;
    private final String password;
    private final Optional<SSLOptions> sslOptions;

    public NativeSSTableLoaderClient(Collection<InetAddress> hosts, int port, String username, String password)
    {
        this(hosts, port, username, password, Optional.<SSLOptions> absent());
    }

    public NativeSSTableLoaderClient(Collection<InetAddress> hosts, int port, String username, String password,
                                     Optional<SSLOptions> sslOptions)
    {
        super();
        this.tables = new HashMap<>();
        this.hosts = hosts;
        this.port = port;
        this.username = username;
        this.password = password;
        this.sslOptions = sslOptions;
    }

    public void init(String keyspace)
    {
        Session session = getSession();
        Metadata metadata = session.getCluster().getMetadata();

        setPartitioner(metadata.getPartitioner());

        Set<TokenRange> tokenRanges = metadata.getTokenRanges();

        Token.TokenFactory tokenFactory = getPartitioner().getTokenFactory();

        for (TokenRange tokenRange : tokenRanges)
        {
            Set<Host> endpoints = metadata.getReplicas(keyspace, tokenRange);
            Range<Token> range = new Range<>(tokenFactory.fromString(tokenRange.getStart().getValue().toString()),
                                             tokenFactory.fromString(tokenRange.getEnd().getValue().toString()));
            for (Host endpoint : endpoints)
                addRangeForEndpoint(range, endpoint.getAddress());
        }

        for (TableMetadata table : metadata.getKeyspace(keyspace).getTables())
            tables.put(table.getName(), convertTableMetadata(table, session));
    }

    public CFMetaData getTableMetadata(String tableName)
    {
        return tables.get(tableName);
    }

    @Override
    public void setTableMetadata(CFMetaData cfm)
    {
        tables.put(cfm.cfName, cfm);
    }

    private CFMetaData convertTableMetadata(TableMetadata table, Session session)
    {
        String keyspace = table.getKeyspace().getName();
        String tableQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = '%s'",
                SystemKeyspace.NAME,
                LegacySchemaTables.COLUMNFAMILIES,
                keyspace);
        ResultSet tableRows = session.execute(tableQuery);
        Row tableRow = tableRows.one();

        return new CFMetaData(keyspace,
                              table.getName(),
                              getCFType(tableRow),
                              getComparator(tableRow),
                              table.getId());
    }

    private ColumnFamilyType getCFType(Row row)
    {
        return ColumnFamilyType.valueOf(row.getString("type"));
    }

    private CellNameType getComparator(Row row)
    {
        AbstractType rawComparator = TypeParser.parse(row.getString("comparator"));
        AbstractType subComparator = row.getString("subcomparator") != null ?
                TypeParser.parse(row.getString("subcomparator")) : null;

        AbstractType<?> fullRawComparator = CFMetaData.makeRawAbstractType(rawComparator, subComparator);
        boolean isDense = row.getBool("is_dense");

        return CellNames.fromAbstractType(fullRawComparator, isDense);
    }

    private Session getSession()
    {
        Cluster.Builder builder = Cluster.builder().addContactPoints(hosts).withPort(port);
        if (sslOptions.isPresent())
            builder.withSSL(sslOptions.get());
        if (username != null && password != null)
            builder = builder.withCredentials(username, password);

        try (Cluster cluster = builder.build())
        {
            return cluster.connect();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to retrieve cluster metadata.", e);
        }
    }
}
