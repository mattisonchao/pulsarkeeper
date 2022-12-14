package io.github.pulsarkeeper.broker.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;

public class HandlerHelpers {
    public static SocketAddress remoteAddress(RoutingContext ctx) {
        return ctx.request().remoteAddress();
    }

    public static String role(RoutingContext ctx) {
        return "";
    }

    public static void ok(RoutingContext ctx, Object obj) {
        ctx.response().setStatusCode(HttpResponseStatus.OK.code());
        ctx.json(obj);
    }

    public static void noContent(RoutingContext ctx) {
        ctx.response()
                .setStatusCode(HttpResponseStatus.NO_CONTENT.code())
                .end();
    }

    public static void created(RoutingContext ctx, Object obj) {
        ctx.response().setStatusCode(HttpResponseStatus.CREATED.code());
        ctx.json(obj);
    }

    public static void notFound(RoutingContext ctx) {
        ctx.response()
                .setStatusCode(HttpResponseStatus.NOT_FOUND.code())
                .setStatusMessage("Resource not found")
                .end();
    }

    public static void badRequest(RoutingContext ctx) {
        ctx.response()
                .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
                .setStatusMessage("Request validation failed")
                .end();
    }

    public static void unprocessableEntity(RoutingContext ctx) {
        ctx.response()
                .setStatusCode(HttpResponseStatus.UNPROCESSABLE_ENTITY.code())
                .setStatusMessage("Resource already existed or in the wrong state.")
                .end();
    }

    public static void conflict(RoutingContext ctx) {
        ctx.response()
                .setStatusCode(HttpResponseStatus.CONFLICT.code())
                .setStatusMessage("Operation conflict with other, try again later.")
                .end();
    }
}
