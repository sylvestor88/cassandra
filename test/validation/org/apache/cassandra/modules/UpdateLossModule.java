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

package org.apache.cassandra.modules;

import java.util.concurrent.Future;

import org.junit.Assert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.HarnessContext;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.htest.Config;

public class UpdateLossModule extends Module
{
    public UpdateLossModule(Config config, HarnessContext context)
    {
        super(config, context);
        executor = new DebuggableThreadPoolExecutor("UpdateLoss", Thread.NORM_PRIORITY);
    }

    public Future validate()
    {
        return newTask(new ValidateTask());
    }

    class ValidateTask implements Runnable
    {
        public void run()
        {
            int lossCount = 0;
            Cluster cluster = Cluster.builder().addContactPoints(bridge.clusterEndpoints()[0]).build();
            Session session = cluster.connect();
            session.execute("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': 1}");
            session.execute("USE k");
            session.execute("CREATE TABLE t ( id int PRIMARY KEY , f double)");
            session.execute("INSERT INTO t (id, f) VALUES (1, 5.5)");
            PreparedStatement update = session.prepare("UPDATE t SET f = ? WHERE id = 1");

            double f1 = 20.5;
            double f2 = 10.5;

            for (int i = 0; i < 1000; i++)
            {
                BoundStatement bound1 = update.bind(f1);
                session.execute(bound1);
                BoundStatement bound2 = update.bind(f2);
                session.execute(bound2);

                ResultSet results = session.execute("SELECT * FROM k.t");

                double f = 0;
                for (Row row : results) {
                    f = row.getDouble("f");
                }

                if (f != f2)
                {
                    lossCount++;
                    harness.signalFailure("UpdateLossModule", "Loss on update " + i + "!! Expected: " + f2 + ", Found: " + f);
                }
            }

            try
            {
                Assert.assertEquals(0, lossCount);
            }
            catch (AssertionError e)
            {
                harness.signalFailure("UpdateLossModule", e.getMessage());
            }
        }
    }
}
