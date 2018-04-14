package com.jacoffee.codebase.socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
    SimpleServer based on nio to improve throughput and optimize resource usage

    + ServerSocketChannel initialization and bind to port
    + Selector initialization
    + register channel with selector

    + selector.select() to select channel

    telnet localhost 20000
    xx
    xx
    yy
    yy
*/
public class NioEchoServer {

  public static void main(String[] args) {
    int port = 20000;
    System.out.println("Listening for connections on port " + port);

    ServerSocketChannel channel = null;
    try {
      channel = ServerSocketChannel.open();
      ServerSocket ss = channel.socket();
      ss.bind(new InetSocketAddress(port));
      channel.configureBlocking(false);

      Selector selector = Selector.open();
      channel.register(selector, SelectionKey.OP_ACCEPT);


      while (true) {
        try {
          // blocking wait, it returns only after at least one channel is selected
          selector.select();
        } catch (IOException ioe) {
          ioe.printStackTrace();
          break;
        }

        Set<SelectionKey> selectKeys = selector.selectedKeys();
        Iterator<SelectionKey> iterator = selectKeys.iterator();

        while (iterator.hasNext()) {
          SelectionKey key = iterator.next();
          iterator.remove();

          try {
            // ready for client connection
            if (key.isAcceptable()) {
              ServerSocketChannel server = (ServerSocketChannel) key.channel();
              SocketChannel client = server.accept();
              System.out.println("Accept connection from " + client);

              client.configureBlocking(false);
              client.register(selector, SelectionKey.OP_WRITE | SelectionKey.OP_READ, ByteBuffer.allocate(1024 * 8));
            }

            // Channel -------->  ByteBuffer
            // Tests whether this key's channel is ready for reading -- i.e client has write something in the channel
            // such as telnet
            if (key.isReadable()) {
              SocketChannel client = (SocketChannel) key.channel();
              ByteBuffer output = (ByteBuffer) key.attachment();
              client.read(output);
            }

            // ByteBuffer ------> Channel
            // Tests whether this key's channel is ready for writing. -- i.e server write what we have received back to channel
            if (key.isWritable()) {
              SocketChannel client = (SocketChannel) key.channel();
              ByteBuffer output = (ByteBuffer) key.attachment();
              // bytebuffer write mode to read mode, i.e make it bytebuffer readable
              output.flip();
              client.write(output);
              output.compact();
            }

          } catch (IOException ex) {
             key.cancel();
             try {
               key.channel().close();
             } catch (IOException ioe) {
                ioe.printStackTrace();
              }
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
        if (channel != null) {
          try {
            channel.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
    }
  }

}
