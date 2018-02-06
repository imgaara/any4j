/*
 * Copyright 2015, gRPC Authors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zero;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import py4j.Gateway;
import py4j.Protocol;
import py4j.ReturnObject;

import java.io.IOException;
import java.lang.reflect.Field;
import java.org.zero.proto.Any4JGrpc;
import java.org.zero.proto.CommonObject;
import java.org.zero.proto.FieldCommand;
import java.org.zero.proto.Response;
import java.org.zero.proto.ReturnObject;
import java.util.logging.Logger;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class HelloWorldServer {
  private static final Logger logger = Logger.getLogger(HelloWorldServer.class.getName());

  private Server server;

  private void start() throws IOException {
    /* The port on which the server should run */
    int port = 50051;
    server = ServerBuilder.forPort(port)
        .addService(new Any4jServerImpl())
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        HelloWorldServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    final HelloWorldServer server = new HelloWorldServer();
    server.start();
    server.blockUntilShutdown();
  }

  static class Any4jServerImpl extends Any4JGrpc.Any4JImplBase {

    private Gateway gateway;

    @Override
    public void getField(FieldCommand fieldCommand, StreamObserver<Response> responseObserver) {

      String objectId = fieldCommand.getTargetObjectId();
      String fieldName = fieldCommand.getFiledName();

      Object object = gateway.getObject(objectId);
      Field field = gateway.getReflectionEngine().getField(object, fieldName);

      logger.finer("Getting field " + fieldName);
      String returnCommand = null;
      Response.Builder response = Response.newBuilder();

      if (field == null) {
        returnCommand = Protocol.getNoSuchFieldOutputCommand();
      } else {
        Object fieldObject = gateway.getReflectionEngine().getFieldValue(object, field);
        ReturnObject rObject = gateway.getReturnObject(fieldObject);
        returnCommand = Protocol.getOutputCommand(rObject);
        response.setReturnObject(toAny4jReturnObject(rObject));
      }

      response.setPy4JResponse(returnCommand);

      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    @Override
    public void setField(FieldCommand fieldCommand, StreamObserver<Response> responseObserver) {
      String objectId = fieldCommand.getTargetObjectId();
      String fieldName = fieldCommand.getFiledName();
      String value = fieldCommand.getValue();

      Object object = gateway.getObject(objectId);
      Object valueObject = Protocol.getObject(value, gateway);
      Field field = gateway.getReflectionEngine().getFieldValue(object, fieldName);

      logger.finer("Setting field " + fieldName);
      String returnCommand = null;
      Response.Builder response = Response.newBuilder();

      if (field == null) {
        returnCommand = Protocol.getNoSuchFieldOutputCommand();
      } else {
        gateway.getReflectionEngine().setFieldValue(object, field, valueObject);
        returnCommand = Protocol.getOutputVoidCommand();
      }

      response.setPy4JResponse(returnCommand);
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    @Override
    public void getServerEntryPoint(FieldCommand request, StreamObserver<Response> responseObserver) {
      Object obj = gateway.getEntryPoint();
      ReturnObject rObject = gateway.getReturnObject(obj);
      String returnCommand = Protocol.getOutputCommand(rObject);

      Response.Builder response = Response.newBuilder();
      response.setReturnObject(toAny4jReturnObject(rObject));
      response.setPy4JResponse(returnCommand);
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    private static CommonObject toAny4jReturnObject(py4j.ReturnObject py4jReturnObj) {
      CommonObject.Builder returnObject =  CommonObject.newBuilder();
      returnObject.setName(py4jReturnObj.getName());
      returnObject.setCommandPart(py4jReturnObj.getCommandPart());
      returnObject.setIsArray(py4jReturnObj.isArray());
      returnObject.setIsDecimal(py4jReturnObj.isDecimal());
      returnObject.setIsError(py4jReturnObj.isError());
      returnObject.setIsIterator(py4jReturnObj.isIterator());
      returnObject.setIsList(py4jReturnObj.isList());
      returnObject.setIsMap(py4jReturnObj.isMap());
      returnObject.setIsNull(py4jReturnObj.isNull());
      returnObject.setIsReference(py4jReturnObj.isReference());
      returnObject.setIsSet(py4jReturnObj.isSet());
      returnObject.setIsVoid(py4jReturnObj.isVoid());
      returnObject.setPrimitiveObject(py4jReturnObj.getPrimitiveObject().toString());
      returnObject.setSize(py4jReturnObj.getSize());
      return returnObject.build();
    }
  }
}