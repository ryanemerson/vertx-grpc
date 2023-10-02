package org.example;

import java.util.Arrays;
import java.util.List;

import io.vertx.core.Vertx;
import io.vertx.core.net.SocketAddress;
import io.vertx.grpc.server.GrpcServer;

public class VertxGrpcClientServer {

   static class Client1 {
      public static void main(String[] args) {
         new VertxGrpcClientServer().run(8080, "localhost:8081");
      }
   }

   static class Client2 {
      public static void main(String[] args) {
         new VertxGrpcClientServer().run(8081, "localhost:8080");
      }
   }

   void run(int serverPort, String... neighbours) {
      Vertx vertx = Vertx.vertx();

      List<SocketAddress> addresses = Arrays.stream(neighbours)
            .map(s -> s.split(":"))
            .map(s -> SocketAddress.inetSocketAddress(Integer.parseInt(s[1]), s[0]))
            .toList();

      GrpcServer rpcServer = GrpcServer.server(vertx);

      rpcServer.callHandler(InfinispanGrpc.getPutMethod(), request -> {
         request
               .last()
               .onSuccess(msg -> {
                  String rspMsg = String.format("localhost:%d added K=%s, V=%s", serverPort, msg.getKey().toStringUtf8(), msg.getValue().toStringUtf8());
                  request.response().end(InfinispanOuterClass.PutResponse.newBuilder().setMessage(rspMsg).build());
               });
      });

      vertx.createHttpServer()
            .requestHandler(rpcServer)
            .listen(serverPort)
            .onFailure(Throwable::printStackTrace);

      GRPCManager grpc = new GRPCManager(vertx);
      vertx.setPeriodic(1000, id -> {
         grpc.putAsync(
                     addresses,
                     ("localhost:" + serverPort).getBytes(), (Long.toString(System.currentTimeMillis())).getBytes())
               .whenComplete(((result, throwable) -> {
                  if (throwable != null)
                     System.err.println(throwable);
                  else
                     result.forEach(System.out::println);
               }));
      });
   }
}
