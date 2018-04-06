package com.jacoffee.codebase.socket;

import org.apache.commons.io.IOUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

// cat cleanup.txt | nc localhost 20000
public class SimpleServerSocket {

  private static ExecutorService pool = Executors.newFixedThreadPool(5);

  private static void handleRequest(Socket connection) {
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      // jdk 1.8
      br.lines().forEach(line -> System.out.println(line));
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(br);
    }
  }

  public static void main(String[] args) throws IOException {
    ServerSocket serverSocket = new ServerSocket(20000);
    // 监听的超时,如果超过设定时间还没有客户端连接,
    serverSocket.setSoTimeout(6000);

    while (true) {
      Socket socket = serverSocket.accept();
      pool.submit(() -> handleRequest(socket));
    }
  }

}
