package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.google.protobuf.ByteString;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.SocketAddress;
import io.vertx.grpc.client.GrpcClient;
import io.vertx.grpc.common.GrpcReadStream;

public class GRPCManager {
   final GrpcClient client;

   public GRPCManager(Vertx vertx) {
      this.client = GrpcClient.client(vertx);
   }

   CompletionStage<List<String>> putAsync(List<SocketAddress> addresses, byte[] key, byte[] value) {

      List<Future<InfinispanOuterClass.PutResponse>> futures = new ArrayList<>(addresses.size());
      for (SocketAddress addr : addresses) {
         futures.add(
               client.request(addr, InfinispanGrpc.getPutMethod())
                     .compose(request -> {
                        request.end(
                              InfinispanOuterClass.PutRequest.newBuilder()
                                    .setKey(ByteString.copyFrom(key))
                                    .setValue(ByteString.copyFrom(value))
                                    .build()
                        );
                        return request.response().compose(GrpcReadStream::last);
                     })
                     .onSuccess(reply -> System.out.println("Succeeded " + reply.getMessage()))
//                     .onFailure(Throwable::printStackTrace)
         );
      }

      List<String> results = new ArrayList<>();
      CompletableFuture<List<String>> future = new CompletableFuture<>();

      Future.any(futures)
            .onComplete(h -> {
               if (h.failed()) {
                  results.add(h.cause().getMessage());
               } else {
                  CompositeFuture cf = h.result();
                  for (int i = 0; i < cf.size(); i++) {
                     InfinispanOuterClass.PutResponse rsp = cf.resultAt(i);
                     results.add(rsp.getMessage());
                  }
               }
               future.complete(results);
            });
      return future;
   }
}
