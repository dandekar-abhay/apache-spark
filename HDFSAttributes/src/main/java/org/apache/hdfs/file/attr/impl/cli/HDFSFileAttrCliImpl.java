package org.apache.hdfs.file.attr.impl.cli;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hdfs.file.attr.HDFSFileAttr;

import java.io.IOException;
import java.io.OutputStream;

public class HDFSFileAttrCliImpl implements HDFSFileAttr {

    FileSystem hdfs;

    String cliFsSetAttrCommand =
            "$HADOOP_HOME/bin/hdfs " +
                    "dfs -getfattr " +
                    "-n" + "#ATTR_LEVEL#.#ATTR_NAME# " +
                    "#PATH#";

    public HDFSFileAttrCliImpl(FileSystem hdfs){
        this.hdfs = hdfs;
    }

    boolean setAttr(Path fileName, String attrLevel, String attrName, String attrValue){

        String command = cliFsSetAttrCommand
                .replace("#ATTR_LEVEL#", attrLevel)
                .replace("#ATTR_NAME#", attrName)
                .replace("#PATH#", fileName.toString());

        try {
            System.out.println("Executing command : " + command);
            Process p = Runtime.getRuntime().exec(command);
            OutputStream output = p.getOutputStream();

            

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }

        return true;
    }

    boolean getAttr(Path fileName, String attrLevel, String attrName, String attrValue){

        return true;
    }

}
