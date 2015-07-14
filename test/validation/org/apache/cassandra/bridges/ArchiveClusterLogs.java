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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ArchiveClusterLogs
{
    public static boolean checkForFolder(String dirPath)
    {
        File file = new File(dirPath);
        return file.exists();
    }

    public static void zipExistingDirectory(String existDir)
    {
        String sourceFolderName = existDir;
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

    private static void compressFiles(ZipOutputStream zos, String folderName, String baseFolderName)
    {
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
                    // Reads up to 1024 bytes of data from the file
                    while ((len = in.read(buffer)) > 0)
                    {
                        // Writes the data to the current ZipEntry
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

    public static void savetempDirectoryPath(File CASSANDRA_DIR, File tmp_Dir)
    {
        File filePath = new File(CASSANDRA_DIR + "/build/test/logs/validation/tempDir.txt");

        try
        {
            if (!filePath.exists())
            {
                filePath.getParentFile().mkdirs();
            }
            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)));
            pw.println(tmp_Dir);
            pw.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String getFullPath(File file, String folder)
    {
        File logFolder = new File(folder);

        if(!logFolder.exists()){
            logFolder.mkdirs();
        }

        return file.getAbsolutePath();
    }

    public static boolean countErrors(String errorLogs)
    {
        int count = 0;
        Pattern p = Pattern.compile("ERROR");
        Matcher m = p.matcher(errorLogs);

        while (m.find()) {
            count++;
        }

        if (count > 6)
            return true;
        else
            return false;
    }

    public static String[] metaDataFilesFormatter(String files)
    {
        files = files.substring(1, files.length()-1);
        String[] list = files.split(", ");
        String[] newlist = new String[list.length];
        int i = 0;

        for(String each : list)
        {
            String edit = each.substring(1, each.length()-1);
            newlist[i] = edit;
            i++;
        }
        return newlist;
    }
}
