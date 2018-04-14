package com.jacoffee.codebase.nio;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.commons.io.IOUtils;

/**
    Basic step for nio

   + create a channel
   + allocate bytebuffer
   + read data from channel to bytebuffer
   + call bytebuffer.flip to switch from write-mode to read-mode
   + read data out of byte buffer
   + call bytebuffer clear to write to bytebuffer again

*/
public class NIORead {

  public static void main(String[] args) throws IOException {
    FileInputStream fis = null;

    try {
      String projectHome = System.getProperty("user.dir");
      String ioFileCopyPath = String.format("%s/src/main/java/com/jacoffee/codebase/nio/IOFileCopy.java", projectHome);
      fis = new FileInputStream(ioFileCopyPath);

      FileChannel channel = fis.getChannel();
      ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 8);

      int bytesRead = 0;
      while ((bytesRead = channel.read(byteBuffer)) > 0) {
        byteBuffer.flip();

        while (byteBuffer.hasRemaining()) {
          // convert to char, then the \t \n could be there
          System.out.print((char)byteBuffer.get());
        }

        byteBuffer.clear();
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(fis);
    }

  }

}
