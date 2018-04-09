package com.jacoffee.codebase.nio;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;

/*
  + copy file from A to B with traditional IO
*/
public class IOFileCopy {

  private IOFileCopy() {}

  private final static int BLKSIZE = 8 * 1024;

  // full file path
  private static void copyFromFile(String original, String destination) {
    try {
      FileInputStream fis = new FileInputStream(original);
      FileOutputStream fos = new FileOutputStream(destination);
      copyFromStream(fis, fos);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  private static void copyFromStream(InputStream is, OutputStream os) {
    byte[] buffer = new byte[BLKSIZE];

    try {
      int bytesRead;
      while ((bytesRead = is.read(buffer)) > 0) {
        os.write(buffer, 0, bytesRead);
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } finally {
      IOUtils.closeQuietly(is);
      IOUtils.closeQuietly(os);
    }
  }

}
