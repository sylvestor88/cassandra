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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.HarnessContext;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.htest.Config;
import org.apache.cassandra.stress.settings.StressSettings;

public class LargeStressWriteModule extends AbstractStressModule
{
    private static final Logger logger = LoggerFactory.getLogger(LargeStressWriteModule.class);

    public LargeStressWriteModule(Config config, HarnessContext context)
    {
        super(config, context, "write n=2M -log file=LargeStressWrite.log");
        executor = new DebuggableThreadPoolExecutor("LargeStressWrite", Thread.NORM_PRIORITY);
    }

    public Future validate()
    {
        Future future = newTask(new StressTask());
        try
        {
            future.get();
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
            harness.signalFailure("LargeStressWriteModule", e.getMessage());
        }

        return newTask(new ValidateTask());
    }

    class ValidateTask implements Runnable
    {
        public void run()
        {

            Cluster cluster = Cluster.builder().addContactPoints(bridge.clusterEndpoints()[0]).build();
            Session session = cluster.connect();

            ResultSet results = session.execute("SELECT * FROM keyspace1.standard1");
            try
            {
                Assert.assertEquals(2000000, results.all().size());
            }
            catch (AssertionError e)
            {
                harness.signalFailure("LargeStressWriteModule", e.getMessage());
            }
        }
    }
}
