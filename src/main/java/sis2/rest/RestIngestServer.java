package sis2.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import sis2.core.IngestCore;
import sis2.core.PublishEventCommand;
import sis2.core.Result;
import sis2.core.ServiceException;

/**
 * REST/JSON baseline for the Ingest API (SIS #1): POST /v1/events over HTTP/1.1 with keep-alive,
 * returns 202 Accepted on success (SIS #1 contract). The idempotency key defaults to event_id in the
 * body; the "Idempotency-Key" header overrides it. Netty is used for both this server and gRPC so the
 * benchmark compares protocols + serialization (HTTP/1.1 + JSON vs HTTP/2 + protobuf), not server engines.
 */
public final class RestIngestServer implements AutoCloseable {

    public static final String PATH = "/v1/events";
    public static final String IDEMPOTENCY_HEADER = "Idempotency-Key";
    public static final ObjectMapper JSON = new ObjectMapper();

    /** JSON body of the request. All fields are optional at the parser level; validated by the core. */
    public record EventBody(String eventId, String userId, String type, String payload) {
    }

    private record Reply(int status, byte[] body, boolean gzip, boolean replayed) {
    }

    private final IngestCore core;
    private final ExecutorService pool;
    private final EventLoopGroup boss = new NioEventLoopGroup(1);
    private final EventLoopGroup workers = new NioEventLoopGroup();
    private final int requestedPort;
    private Channel channel;

    public RestIngestServer(IngestCore core, int port, int threads) {
        this.core = core;
        this.requestedPort = port;
        this.pool = Executors.newFixedThreadPool(threads);
    }

    public RestIngestServer start() throws IOException {
        try {
            channel = new ServerBootstrap()
                    .group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new HttpServerCodec(), new HttpObjectAggregator(64 << 20), new Handler());
                        }
                    })
                    .bind(requestedPort).sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        return this;
    }

    public int port() {
        return ((java.net.InetSocketAddress) channel.localAddress()).getPort();
    }

    @Override
    public void close() {
        if (channel != null) channel.close().syncUninterruptibly();
        boss.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        workers.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        pool.shutdownNow();
    }

    private final class Handler extends SimpleChannelInboundHandler<FullHttpRequest> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            // copy what we need NOW: the request buffer is released when this method returns
            boolean post = req.method().name().equals("POST");
            byte[] raw = new byte[req.content().readableBytes()];
            req.content().readBytes(raw);
            String overrideKey = req.headers().get(IDEMPOTENCY_HEADER);
            boolean gzipIn = "gzip".equalsIgnoreCase(req.headers().get(HttpHeaderNames.CONTENT_ENCODING));
            boolean gzipOut = String.valueOf(req.headers().get(HttpHeaderNames.ACCEPT_ENCODING)).contains("gzip");
            boolean keepAlive = HttpUtil.isKeepAlive(req);

            // business logic runs on the handler pool (same size as the gRPC executor), never on the event loop
            pool.execute(() -> {
                Reply r = post ? process(raw, overrideKey, gzipIn, gzipOut) : error(405, "POST only");
                byte[] out = r.body();
                if (r.gzip()) out = gzip(out);
                FullHttpResponse resp = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(r.status()), Unpooled.wrappedBuffer(out));
                resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
                resp.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, out.length);
                if (r.gzip()) resp.headers().set(HttpHeaderNames.CONTENT_ENCODING, "gzip");
                if (r.replayed()) resp.headers().set("Idempotent-Replayed", "true");
                if (keepAlive) {
                    resp.headers().set(HttpHeaderNames.CONNECTION, "keep-alive");
                    ctx.writeAndFlush(resp);
                } else {
                    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private Reply process(byte[] raw, String overrideKey, boolean gzipIn, boolean gzipOut) {
        try {
            if (gzipIn) raw = new GZIPInputStream(new ByteArrayInputStream(raw)).readAllBytes();
            EventBody b;
            try {
                b = JSON.readValue(raw, EventBody.class);
            } catch (IOException bad) {
                return new Reply(400, json(Map.of("error", "malformed JSON")), gzipOut, false);
            }
            try {
                Result r = core.publishEvent(overrideKey,
                        new PublishEventCommand(b.eventId(), b.userId(), b.type(), b.payload()),
                        () -> false);
                // 202 Accepted per SIS #1 contract: POST /v1/events -> 202
                return new Reply(202, json(Map.of("eventId", r.eventId(), "accepted", r.accepted(),
                        "replayed", r.replayed())), gzipOut, r.replayed());
            } catch (ServiceException e) {
                return new Reply(status(e), json(Map.of("error", String.valueOf(e.getMessage()))), gzipOut, false);
            }
        } catch (IOException | RuntimeException e) {
            return error(500, "internal error");
        }
    }

    static int status(ServiceException e) {
        return switch (e.kind()) {
            case INVALID_ARGUMENT -> 400;
            case KEY_REUSED -> 422;
            case UNAVAILABLE -> 503;
            case CANCELLED -> 499;
        };
    }

    private static Reply error(int status, String msg) {
        return new Reply(status, json(Map.of("error", msg)), false, false);
    }

    private static byte[] json(Object o) {
        try {
            return JSON.writeValueAsBytes(o);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] gzip(byte[] in) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(in.length / 2 + 32);
        try (GZIPOutputStream g = new GZIPOutputStream(bos)) {
            g.write(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return bos.toByteArray();
    }
}
