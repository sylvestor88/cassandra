/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.cassandra.bridges.ccmbridge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.bridges.Bridge;
import org.apache.cassandra.htest.Config;

import org.apache.commons.io.FileUtils;

public class CCMBridge extends Bridge
{

    private int testNodeCount;
    static final File CASSANDRA_DIR = new File("./");

    private final Runtime runtime = Runtime.getRuntime();
    private final File ccmDir;
    private final String DEFAULT_CLUSTER_NAME = "validation";

    private static final Logger logger = LoggerFactory.getLogger(CCMBridge.class);

    public CCMBridge(Config config)
    {
        this(config.nodeCount);
        if (config.cassandrayaml != null)
            updateConf(config.cassandrayaml);
        start();
    }

    public CCMBridge(int nodeCount)
    {
        testNodeCount = nodeCount;
        this.ccmDir = Files.createTempDir();
        execute("ccm create %s -n %d --install-dir %s", DEFAULT_CLUSTER_NAME, nodeCount, CASSANDRA_DIR);
    }

    public void captureLogs(String testName)
    {
        stop();
        preserveClusterLogs(testName);
    }

    public void destroy()
    {
        execute("ccm remove");
    }

    public void start()
    {
        execute("ccm start");
    }

    public void stop()
    {
        execute("ccm stop");
    }

    public void forceStop()
    {
        execute("ccm stop --not-gently");
    }

    public String readClusterLogs()
    {
        String result = executeAndRead("ccm checklogerror");
        return result;
    }

    public void updateConf(Map<String, String> options)
    {
        for (String key : options.keySet())
            execute("ccm updateconf %s:%s", key, options.get(key));
    }

    private void execute(String command, Object... args)
    {
        try
        {
            String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);
            int retValue = p.waitFor();

            if (retValue != 0)
            {
                BufferedReader outReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                String line = outReader.readLine();
                while (line != null)
                {
                    logger.info("out> " + line);
                    line = outReader.readLine();
                }
                line = errReader.readLine();
                while (line != null)
                {
                    logger.error("err> " + line);
                    line = errReader.readLine();
                }
                throw new RuntimeException();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void executeAndPrint(String command, Object... args)
    {
        try
        {
            String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);
            int retValue = p.waitFor();

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
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private String executeAndRead(String command, Object... args)
    {
        try
        {
            String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);

            BufferedReader outReaderOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = outReaderOutput.readLine();
            String output = null;

            while (line != null)
            {
                output += line + "\n";
                line = outReaderOutput.readLine();
            }

            return output;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void preserveClusterLogs(String testName){

        int count = 1;
        String clusterLogsPath = ccmDir + "/" + DEFAULT_CLUSTER_NAME;
        String folderName = testName;
        String existingFolder = CASSANDRA_DIR + "/build/test/logs/validation/" + folderName;

        if(checkForFolder(existingFolder))
        {
            zipExistingDirectory(existingFolder);
        }

        while (count <= testNodeCount){

            File sourceFile = new File(clusterLogsPath + "/node" + count + "/logs" + "/system.log");

            File destFile = new File(CASSANDRA_DIR + "/build/test/logs/validation/" + folderName + "/node" + count + ".log");

            try
            {
                FileUtils.copyFile(sourceFile, destFile);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            count++;
        }

    }

    public boolean checkForFolder(String dirPath){

        File file = new File(dirPath);
        return file.exists();
    }

    public void zipExistingDirectory(String existDir){

        String sourceFolderName =  existDir;
        long unixTime = System.currentTimeMillis() / 1000L;
        String outputFileName = existDir + "_" + unixTime + ".zip";

        try
        {
            FileOutputStream fos = new FileOutputStream(outputFileName);
            ZipOutputStream zos = new ZipOutputStream(fos);
            zos.setLevel(9);
            compressFiles(zos, sourceFolderName, sourceFolderName);
            zos.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void compressFiles(ZipOutputStream zos,String folderName,String baseFolderName){

        File f = new File(folderName);

        try
        {
            if (f.exists())
            {

                if (f.isDirectory())
                {
                    File f2[] = f.listFiles();
                    for (int i = 0; i < f2.length; i++)
                    {
                        compressFiles(zos, f2[i].getAbsolutePath(), baseFolderName);
                    }
                }
                else
                {
                    String entryName = folderName.substring(baseFolderName.length() + 1, folderName.length());
                    ZipEntry ze = new ZipEntry(entryName);
                    zos.putNextEntry(ze);
                    FileInputStream in = new FileInputStream(folderName);
                    int len;
                    byte buffer[] = new byte[1024];
                    while ((len = in.read(buffer)) > 0)
                    {
                        zos.write(buffer, 0, len);
                    }
                    in.close();
                    zos.closeEntry();
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
