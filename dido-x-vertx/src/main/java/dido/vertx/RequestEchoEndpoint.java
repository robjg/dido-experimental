package dido.vertx;

import dido.vertx.util.FormatterOut;
import dido.vertx.util.SectionTextOut;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.UserContext;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.Principal;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class RequestEchoEndpoint implements Supplier<Function<RoutingContext, Future<?>>> {

    @Override
    public Function<RoutingContext, Future<?>> get() {

        return new Function<>() {

            public Future<?> apply(RoutingContext routingContext) {

                HttpServerRequest request = routingContext.request();

                StringBuilder sb = new StringBuilder();

                SectionTextOut.of(sb)
                        .out(httpServerRequest(request))
                        .out(userContext(routingContext.userContext()));

                return routingContext.response()
                        .putHeader("content-type", "text/plain")
                        .end(sb.toString());
            }

            @Override
            public String toString() {
                return "RequestEchoEndpoint";
            }
        };
    }

    public static Consumer<FormatterOut> httpServerRequest(HttpServerRequest request) {

        String name = "Request";

        return out -> {
            if (request == null) {
                out.value(name, null);
            } else {
                out.nested(name, req -> req
                        .value("Path", request.path())
                        .out(sslSession(request.sslSession())));
            }
        };
    }

    public static Consumer<FormatterOut> userContext(UserContext userContext) {

        String name = "UserContext";

        return out -> {
            out.value(name, Optional.ofNullable(userContext)
                    .map(UserContext::get)
                    .map(User::subject)
                    .orElse(null));
        };
    }

    public static Consumer<FormatterOut> sslSession(SSLSession sslSession) {

        String name = "SslSession";

        return out -> {

            if (sslSession == null) {
                out.value(name, null);
            } else {
                try {
                    out.out(principal(sslSession.getPeerPrincipal()));
                    out.out(certificates("PeerCertificates",
                            sslSession.getPeerCertificates()));
                } catch (SSLPeerUnverifiedException e) {
                    out.value(name, "Exception: " + e);
                }
            }
        };
    }

    public static Consumer<FormatterOut> certificates(String certificateTitle,
                                                      java.security.cert.Certificate[] certificates) {

        return out -> {
            out.repeating(certificateTitle, repeating ->
                    Arrays.stream(certificates).forEach(cert ->
                            repeating.item(certificate(cert))));
        };
    }

    public static Consumer<FormatterOut> certificate(java.security.cert.Certificate certificate) {
        return out -> {
            out.value("Certificate", certificate.toString());
        };
    }

    public static Consumer<FormatterOut> principal(Principal principal) {

        return out ->
                out.value("Principal", Optional.ofNullable(principal)
                        .map(Principal::getName).orElse("null"));
    }
}
