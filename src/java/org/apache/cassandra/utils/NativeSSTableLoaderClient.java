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

import java.net.InetAddress;
import java.util.*;

import com.datastax.driver.core.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;

public class NativeSSTableLoaderClient extends SSTableLoader.Client
{
    protected final Map<String, CFMetaData> tables;
    private final Collection<InetAddress> hosts;
    private final int port;
    private final String username;
    private final String password;

    public NativeSSTableLoaderClient(Collection<InetAddress> hosts, int port, String username, String password)
    {
        super();
        this.tables = new HashMap<>();
        this.hosts = hosts;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public void init(String keyspace)
    {
        Metadata metadata = fetchClusterMetadata();

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
            tables.put(table.getName(), convertTableMetadata(table));
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

    private CFMetaData convertTableMetadata(TableMetadata table)
    {
        return new CFMetaData(table.getKeyspace().getName(),
                              table.getName(),
                              ColumnFamilyType.Standard,
                              getComparator(table),
                              table.getId());
    }

    // FIXME this is not smart enough
    private CellNameType getComparator(TableMetadata table)
    {
        AbstractType type;
        if (table.getClusteringColumns().size() > 1)
        {
            ArrayList<AbstractType> types = new ArrayList<>();
            for (ColumnMetadata column : table.getClusteringColumns())
                types.add(TypeParser.parseCqlName(column.getType().toString()));
            type = CompositeType.getInstance((AbstractType[]) types.toArray());
        }
        else
        {
            type = TypeParser.parseCqlName(table.getClusteringColumns().get(0).getType().toString());
        }

        return CellNames.fromAbstractType(type, table.getOptions().isCompactStorage());
    }

    private Metadata fetchClusterMetadata()
    {
        Cluster.Builder builder = Cluster.builder().addContactPoints(hosts).withPort(port);
        if (username != null && password != null)
            builder = builder.withCredentials(username, password);

        try (Cluster cluster = builder.build())
        {
            cluster.connect();
            return cluster.getMetadata();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to retrieve cluster metadata.", e);
        }
    }
}
