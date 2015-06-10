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
package org.apache.cassandra;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import org.apache.cassandra.bridges.Bridge;
import org.apache.cassandra.bridges.ccmbridge.CCMBridge;
import org.apache.cassandra.htest.Config;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.modules.*;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

@RunWith(Parameterized.class)
public class HarnessTester
{
    public static final String MODULE_PACKAGE = "org.apache.cassandra.modules.";
    private String yaml;
    private Bridge cluster;

    @Parameterized.Parameters
    public static Collection<Object[]> discoverTests()
    {
        return Arrays.asList(new Object[][] {
                    {"/Users/philipthompson/cstar/cassandra/test/validation/org/apache/cassandra/htest/test.yaml"},
                    {"/Users/philipthompson/cstar/cassandra/test/validation/org/apache/cassandra/htest/test2.yaml"}});
    }

    public HarnessTester(String yamlParameter)
    {
        yaml = yamlParameter;
    }

    @Test
    public void harness()
    {
        Config config = loadConfig(getConfigURL(yaml));
        cluster = new CCMBridge(config.nodeCount);
        ArrayList<Module> modules = new ArrayList<>();
        for (String moduleName: config.modules)
        {
            Module module = reflectModuleByName(moduleName, config);
            modules.add(module);
        }

        ArrayList<Future> futures = new ArrayList<>(modules.size());
        for (Module module : modules)
        {
            Future future = module.validate();
            futures.add(future);
        }

        try
        {
            for (Future future : futures)
            {
                future.get();
            }
        }
        catch (Exception e)
        {
            Assert.assertTrue(false);
        }
    }

    @After
    public void tearDown()
    {
        cluster.destroy();
    }

    public Module reflectModuleByName(String moduleName, Config config)
    {
        try
        {
            return (Module) Class.forName(MODULE_PACKAGE + moduleName).getDeclaredConstructor(Config.class).newInstance(config);
        }
        // ClassNotFoundException
        // NoSuchMethodException
        // InvocationTargetException
        // InstantiationException
        // IllegalAccessException
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public Config loadConfig(URL url)
    {
        try
        {
            byte[] configBytes;
            try (InputStream is = url.openStream())
            {
                configBytes = ByteStreams.toByteArray(is);
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
            org.yaml.snakeyaml.constructor.Constructor constructor = new org.yaml.snakeyaml.constructor.Constructor(Config.class);
            Yaml yaml = new Yaml(constructor);
            Config result = yaml.loadAs(new ByteArrayInputStream(configBytes), Config.class);
            return result;
        }
        catch (YAMLException e)
        {
            throw new ConfigurationException("Invalid yaml: " + url, e);
        }
    }

    static URL getConfigURL(String yamlPath)
    {
        URL url;
        try
        {
            url = new URL("file:" + File.separator + File.separator + yamlPath);
            url.openStream().close(); // catches well-formed but bogus URLs
            return url;
        }
        catch (Exception e)
        {
            throw new AssertionError("Yaml path was invalid", e);
        }
    }
}
