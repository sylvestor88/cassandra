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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.HarnessContext;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.htest.Config;

public class UpdateCounterModule extends Module
{
    private static final Logger logger = LoggerFactory.getLogger(UpdateCounterModule.class);
    public UpdateCounterModule(Config config, HarnessContext context)
    {
        super(config, context);
        executor = new DebuggableThreadPoolExecutor("UpdateCounter", Thread.NORM_PRIORITY);
    }

    public Future validate()
    {
        return newTask(new ValidateTask());
    }

    class ValidateTask implements Runnable
    {
        public void run()
        {
            Cluster cluster = Cluster.builder().addContactPoints(bridge.clusterEndpoints()[0]).build();
            Session session = cluster.connect();
            session.execute("USE k");
            session.execute("CREATE TABLE c (id int PRIMARY KEY, mycounter counter)");
            PreparedStatement update = session.prepare("UPDATE c SET mycounter=mycounter+1 WHERE id=1");
            Statement select = QueryBuilder.select().all().from("t").limit(100);
            update.setConsistencyLevel(ConsistencyLevel.QUORUM);
            boolean exception = false;

            while(!exception)
            {
                ResultSet results = session.execute(select);
                if(results != null)
                {
                    for (Row row : results)
                    {
                        try
                        {
                            BoundStatement bound = update.bind();
                            session.execute(bound);
                        }
                        catch(Exception e)
                        {
                            exception = true;
                            logger.error(e.getMessage());
                        }
                    }
                }
            }

            try
            {
                Assert.assertEquals(true, exception);
            }
            catch (AssertionError e)
            {
                harness.signalFailure("UpdateCounterModule", e.getMessage());
            }

        }
    }
}
