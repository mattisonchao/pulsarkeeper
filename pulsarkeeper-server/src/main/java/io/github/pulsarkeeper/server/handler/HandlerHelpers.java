package io.github.pulsarkeeper.server.handler;

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
}
