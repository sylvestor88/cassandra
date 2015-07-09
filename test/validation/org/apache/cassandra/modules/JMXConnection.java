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

import java.util.HashMap;
import java.util.Map;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JMXConnection
{
    private static final String FMT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private final String host, username, password;
    private final int port;
    private JMXConnector jmxc;
    private MBeanServerConnection mbeanServerConn;

    JMXConnection(String host, int port, String username, String password)
    {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        connect();
    }

    private void connect()
    {
        try
        {
            JMXServiceURL jmxUrl = new JMXServiceURL(String.format(FMT_URL, host, port));
            Map<String, Object> env = new HashMap<String, Object>();

            if (username != null)
                env.put(JMXConnector.CREDENTIALS, new String[]{ username, password });

            jmxc = JMXConnectorFactory.connect(jmxUrl, env);
            mbeanServerConn = jmxc.getMBeanServerConnection();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        try
        {
            jmxc.close();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public Integer getObjectMetric(String oName, String attribute)
    {
        Integer metric;

        try
        {
            ObjectName name = new ObjectName(oName);
            metric = (Integer) mbeanServerConn.getAttribute(name, attribute);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        return metric;
    }
}
