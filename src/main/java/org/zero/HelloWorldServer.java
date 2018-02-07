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
import io.netty.handler.codec.http.HttpResponseStatus;
import py4j.Gateway;
import py4j.Protocol;
import py4j.ReturnObject;
import py4j.reflection.MethodInvoker;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.zero.proto.AJMethod;
import org.zero.proto.AJObject;
import org.zero.proto.AJVoid;
import org.zero.proto.Any4JGrpc;
import org.zero.proto.CallCommand;
import org.zero.proto.FieldGetCommand;
import org.zero.proto.FieldSetCommand;
import org.zero.proto.Response;
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

    private Gateway gateway = new Gateway(this);

    public int add(int a, int b) {
      return a + b;
    }

    @Override
    public void getField(FieldGetCommand fieldCommand, StreamObserver<Response> responseObserver) {
      AJObject ajObject = fieldCommand.getTargetObject();

      Response.Builder response = Response.newBuilder();
      if (!ajObject.getIsReference()) {
        response.setStatusCode(HttpResponseStatus.BAD_REQUEST.codeAsText().toString());
        response.setMessage("not a object for " + ajObject.getName());
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
        return;
      }

      String fieldName = fieldCommand.getFiledName();
      Object object = gateway.getObject(ajObject.getObjectId());

      AJObject retObj = deepFetch(object, fieldName, fieldCommand.getAutoFetchAll());

      response.setReturnObject(retObj);
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    @Override
    public void setField(FieldSetCommand fieldCommand, StreamObserver<Response> responseObserver) {
      String objectId = fieldCommand.getTargetObject().getObjectId();
      String fieldName = fieldCommand.getFiledName();
      AJObject value = fieldCommand.getValue();

      Object object = gateway.getObject(objectId);
      Object valueObject = Protocol.getObject(value.getName(), gateway);
      Field field = gateway.getReflectionEngine().getField(object, fieldName);

      logger.finer("Setting field " + fieldName);
      Response.Builder response = Response.newBuilder();

      if (field == null) {
        response.setMessage(Protocol.getNoSuchFieldOutputCommand());
      } else {
        gateway.getReflectionEngine().setFieldValue(object, field, valueObject);
      }

      response.setStatusCode(HttpResponseStatus.OK.codeAsText().toString());
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    @Override
    public void getServerEntryPoint(AJVoid request, StreamObserver<Response> responseObserver) {
      Object obj = gateway.getEntryPoint();
      ReturnObject rObject = gateway.getReturnObject(obj);

      Response.Builder response = Response.newBuilder();
      response.setStatusCode(HttpResponseStatus.OK.codeAsText().toString());
      response.setReturnObject(toAJObjectBuilder(rObject).build());
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    @Override
    public void call(CallCommand request, StreamObserver<Response> responseObserver) {
      Object object = gateway.getObject(request.getTargetObject().getObjectId());

      Object[] args = request.getArgumentsList().stream().map(
              a -> Protocol.getObject(a.getCommandPart(), gateway)
      ).toArray();

      MethodInvoker invoker = gateway.getReflectionEngine().getMethod(object, request.getMethodName(), args);

      Object ret = gateway.getReflectionEngine().invoke(object, invoker, args);

      Response.Builder response = Response.newBuilder();
      ReturnObject rObject = gateway.getReturnObject(ret);

      response.setStatusCode(HttpResponseStatus.OK.codeAsText().toString());
      response.setReturnObject(toAJObjectBuilder(rObject).build());
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    public static AJObject.Builder toAJObjectBuilder(py4j.ReturnObject py4jReturnObj) {
      AJObject.Builder returnObject =  AJObject.newBuilder();
      if (py4jReturnObj.isReference()) {
        returnObject.setObjectId(py4jReturnObj.getName());
      }

      if (py4jReturnObj.getName() != null) {
        returnObject.setName(py4jReturnObj.getName());
      }
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
      if (py4jReturnObj.getPrimitiveObject() != null) {
        returnObject.setPrimitiveObject(py4jReturnObj.getPrimitiveObject().toString());
      }
      returnObject.setSize(py4jReturnObj.getSize());

      if (py4jReturnObj.getCommandPart() != null) {
        returnObject.setCommandPart(py4jReturnObj.getCommandPart());
      }
      return returnObject;
    }

    private static AJMethod toAJMethod(Method method) {
      AJMethod.Builder builder = AJMethod.newBuilder();
      builder.setName(method.getName());
      builder.setVisibility("public");
      if (Modifier.isStatic(method.getModifiers())) {
        builder.setIsStatic(true);
      }
      return builder.build();
    }

    private AJObject deepFetch(Object object, String fieldName, boolean autoFetchAll) {
      Field field = gateway.getReflectionEngine().getField(object, fieldName);

      if (field == null) {
        return null;
      }

      logger.finer("Getting field " + fieldName);

      Object fieldObject = gateway.getReflectionEngine().getFieldValue(object, field);
      ReturnObject rObject = gateway.getReturnObject(fieldObject);
      AJObject.Builder builder = toAJObjectBuilder(rObject);

      if (autoFetchAll) {
        for (String publicFieldName : gateway.getReflectionEngine().getPublicFieldNames(fieldObject)) {
          AJObject oneField = deepFetch(fieldObject, publicFieldName, autoFetchAll);
          builder.addFields(oneField);
        }

        for (String publicFieldName : gateway.getReflectionEngine().getPublicStaticFieldNames(fieldObject.getClass())) {
          AJObject oneStaticField = deepFetch(fieldObject.getClass(), publicFieldName, autoFetchAll);
          builder.addStaticFields(oneStaticField);
        }

        for (String methodName : gateway.getReflectionEngine().getPublicMethodNames(fieldObject)) {
          Method method = gateway.getReflectionEngine().getMethod(object.getClass(), methodName);
          builder.addMethods(toAJMethod(method));
        }

        for (String methodName : gateway.getReflectionEngine().getPublicStaticMethodNames(fieldObject.getClass())) {
          Method staticMethod = gateway.getReflectionEngine().getMethod(object.getClass(), methodName);
          builder.addMethods(toAJMethod(staticMethod));
        }
      }

      return builder.build();
    }
  }
}