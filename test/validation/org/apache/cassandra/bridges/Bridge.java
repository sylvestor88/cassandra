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
package org.apache.cassandra.bridges;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Bridge
{
    private static final Logger logger = LoggerFactory.getLogger(Bridge.class);

    protected static final File CASSANDRA_DIR = new File("./");
    protected final Runtime runtime = Runtime.getRuntime();

    public abstract void stop();
    public abstract void destroy();
    public abstract String readClusterLogs();
    public abstract void captureLogs(String testName);

    public void nodeTool(int node, String command, String arguments)
    {
        try
        {
            String fullCommand = "ccm node" + node + " nodetool " + command + " " + arguments;
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand);

            BufferedReader outReaderOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = outReaderOutput.readLine();

            while (line != null)
            {
                System.out.println(line);
                line = outReaderOutput.readLine();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

    }
}
