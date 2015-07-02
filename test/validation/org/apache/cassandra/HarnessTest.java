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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridges.Bridge;
import org.apache.cassandra.bridges.ccmbridge.CCMBridge;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.htest.Config;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.modules.Module;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

@RunWith(Parameterized.class)
public class HarnessTest
{
    private static final Logger logger = LoggerFactory.getLogger(HarnessTest.class);
    public static final String MODULE_PACKAGE = "org.apache.cassandra.modules.";
    private String yaml;
    private Bridge cluster;
    private Map<String, List<String>> failures = new HashMap<>();
    private DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor("Harness", Thread.NORM_PRIORITY);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> discoverTests()
    {

        File folder = new File("../cassandra/test/validation/org/apache/cassandra/htest");
        File[] listOfFiles = folder.listFiles();

        Collection<Object[]> result = new ArrayList<Object[]>();

        for (int i = 0; i < listOfFiles.length; i++)
        {
            File file = listOfFiles[i];
            if (file.isFile() && file.getName().endsWith(".yaml"))
            {
                String content = FileUtils.getCanonicalPath(file);
                result.add(new Object[]{content});
            }
        }

        return result;
    }

    public HarnessTest(String yamlParameter)
    {
        yaml = yamlParameter;
    }

    @Test
    public void harness()
    {
        Config config = loadConfig(getConfigURL(yaml));
        cluster = new CCMBridge(config);
        HarnessContext context = new HarnessContext(this, cluster);
        for (String[] moduleGroup : config.modules)
        {
            ArrayList<Module> modules = new ArrayList<>();
            for (String moduleName : moduleGroup)
            {
                Module module = reflectModuleByName(moduleName, config, context);
                modules.add(module);
            }

            runModuleGroup(modules);
        }
    }

    @After
    public void tearDown()
    {
        for(String moduleName : failures.keySet())
        {
            failures.get(moduleName).forEach(logger::error);
        }
        cluster.stop();
        cluster.captureLogs(getTestName(yaml));
        String result = cluster.readClusterLogs();
        cluster.destroy();
        Assert.assertTrue(result, result == "");
        if(!failures.isEmpty())
            Assert.fail();
    }

    public void signalFailure(String moduleName, String message)
    {
        newTask(new FailureTask(moduleName, message));
    }

    private Future newTask(Runnable task)
    {
        return executor.submit(task);
    }

    public void runModuleGroup(ArrayList<Module> modules)
    {
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
            throw new RuntimeException(e);
        }
    }

    public Module reflectModuleByName(String moduleName, Config config, HarnessContext context)
    {
        try
        {
            return (Module) Class.forName(MODULE_PACKAGE + moduleName)
                                 .getDeclaredConstructor(new Class[]{Config.class, HarnessContext.class}).newInstance(config, context);
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

    static String getTestName(String yamlPath)
    {
        Path p = Paths.get(yamlPath);
        String file = p.getFileName().toString();
        String testName = file.substring(0, file.lastIndexOf('.'));
        return testName;
    }

    class FailureTask implements Runnable
    {
        private String moduleName;
        private String message;

        public FailureTask(String moduleName, String message)
        {
            this.moduleName = moduleName;
            this.message = message;
        }

        public void run()
        {
            if (failures.containsKey(moduleName))
            {
                failures.get(moduleName).add(message);
            }
            else
            {
                ArrayList<String> failure = new ArrayList<>();
                failure.add(message);
                failures.put(moduleName, failure);
            }
        }
    }
}
