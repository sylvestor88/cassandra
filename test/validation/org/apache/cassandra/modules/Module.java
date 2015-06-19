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

import java.util.concurrent.Future;

import org.apache.cassandra.bridges.Bridge;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.htest.Config;

public abstract class Module
{
    DebuggableThreadPoolExecutor executor;
    final Config config;
    final Bridge bridge;

    public Module(Config config, Bridge bridge)
    {
        this.config = config;
        this.bridge = bridge;
    }

    public abstract Future validate();

    protected Future newTask(Runnable task)
    {
        return executor.submit(task);
    }

}
