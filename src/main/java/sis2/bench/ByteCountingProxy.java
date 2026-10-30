package sis2.bench;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Transparent TCP proxy that counts the bytes that really cross the "network" in each direction
 * (HTTP headers, HTTP/2 frames, SETTINGS, WINDOW_UPDATE, gzip... everything; plaintext, no TLS).
 */
public final class ByteCountingProxy implements AutoCloseable {
    public final AtomicLong clientToServer = new AtomicLong();
    public final AtomicLong serverToClient = new AtomicLong();

    private final ServerSocket listener;
    private final int targetPort;
    private volatile boolean closed;

    public ByteCountingProxy(int targetPort) throws IOException {
        this.targetPort = targetPort;
        this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread t = new Thread(this::acceptLoop, "proxy-accept");
        t.setDaemon(true);
        t.start();
    }

    public int port() {
        return listener.getLocalPort();
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket client = listener.accept();
                Socket server = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                client.setTcpNoDelay(true);
                server.setTcpNoDelay(true);
                pump(client, server, clientToServer, "c2s");
                pump(server, client, serverToClient, "s2c");
            } catch (IOException e) {
                if (!closed) e.printStackTrace();
            }
        }
    }

    private static void pump(Socket from, Socket to, AtomicLong counter, String name) {
        Thread t = new Thread(() -> {
            byte[] buf = new byte[64 * 1024];
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                int n;
                while ((n = in.read(buf)) >= 0) {
                    counter.addAndGet(n);
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException ignored) {
                // connection closed
            } finally {
                try { from.close(); } catch (IOException ignored) { }
                try { to.close(); } catch (IOException ignored) { }
            }
        }, "proxy-" + name);
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        listener.close();
    }
}
