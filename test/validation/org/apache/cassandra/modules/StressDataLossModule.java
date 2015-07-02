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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.bridges.Bridge;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.htest.Config;
import org.apache.cassandra.stress.settings.StressSettings;

public class StressDataLossModule extends AbstractStressModule
{
    private static final Logger logger = LoggerFactory.getLogger(StressDataLossModule.class);

    public StressDataLossModule(Config config, Bridge bridge)
    {
        super(config, bridge, StressSettings.parse(new String[]{ "write", "n=2M", "-log", "file=StressDataLoss.log" }));
        executor = new DebuggableThreadPoolExecutor(2, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory("LargeStressWrite", Thread.NORM_PRIORITY));
    }

    @Override
    public Future validate()
    {
        Future setupFuture = newTask(stress(StressSettings.parse(new String[]{"write", "n=10K"})));
        try
        {
            setupFuture.get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        Future stressFuture = newTask(stress(this.settings));
        Future dataFuture = newTask(new DataLossTask());
        try
        {
            dataFuture.get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        return stressFuture;
    }

    class DataLossTask implements Runnable
    {
        public void run()
        {
            Cluster cluster = Cluster.builder().addContactPoints(bridge.clusterEndpoints()[0]).build();
            Session session = cluster.connect();
            session.execute("USE keyspace1");
            for(int i = 0; i < 10; i++)
            {
                session.execute("TRUNCATE standard1");
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                }
            }
        }
    }
}
