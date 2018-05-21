package org.apache.hdfs.file.attr;

import org.apache.hadoop.fs.Path;

public interface HDFSFileAttr {

    boolean setAttr(Path fileName, String attrLevel, String attrName, String attrValue);

    boolean getAttr(Path fileName, String attrLevel, String attrName, String attrValue);

}
