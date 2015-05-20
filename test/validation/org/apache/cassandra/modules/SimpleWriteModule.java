/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.modules;

import java.lang.Thread;
import java.util.concurrent.Future;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.htest.Config;

public class SimpleWriteModule extends Module
{

    public SimpleWriteModule(Config config)
    {
        super(config);
        executor = new DebuggableThreadPoolExecutor("SimpleWrite", Thread.NORM_PRIORITY);
    }

    public Future validate()
    {
        return newTask(new ValidateTask());
    }

    class ValidateTask implements Runnable
    {
        public void run()
        {
            Cluster cluster = Cluster.builder().addContactPoints("127.0.0.1").build();
            Session session = cluster.connect();
            session.execute("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': 1}");
            session.execute("USE k");
            session.execute("CREATE TABLE t ( id int PRIMARY KEY , v int)");

            PreparedStatement query = session.prepare("INSERT INTO t (id, v) VALUES (?, ?)");

            for (int i = 0; i < 10000; i++)
            {
                BoundStatement bound = query.bind(i, i);
                session.execute(bound);
            }

            ResultSet results = session.execute("SELECT * FROM k.t");
            //TODO: Add a way to pass messages back
            //return results.all().size() == 10000;
        }
    }
}
