package net.rageevent.governor.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import net.rageevent.governor.api.EnvelopeAgent;
import net.rageevent.governor.api.EnvelopeCarrier;
import net.rageevent.governor.api.EnvelopeConnection;
import net.rageevent.governor.api.exceptions.GovernorException;
import net.rageevent.governor.api.protocol.ConnectionListener;
import net.rageevent.governor.api.protocol.ConnectionListeners;
import net.rageevent.governor.api.protocol.EnvelopeEndpoint;
import net.rageevent.governor.api.protocol.EnvelopeRegistry;
import net.rageevent.governor.api.protocol.internal.EnvelopeChannelInit;
import net.rageevent.governor.api.shipments.system.HandshakeRequest;
import net.rageevent.governor.server.modules.NetworkModule;
import net.rageevent.governor.server.players.PlayerTracker;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The Overseer hub: the standalone program every proxy and server connects to. It always tracks servers
 * ({@link #connections()}) and players ({@link #players()}); features such as friends and queues are modules.
 * <pre>{@code
 * NetworkServer hub = new NetworkServer(49152);
 * hub.install(new FriendsModule(hub))
 *    .install(new QueueModule(hub))
 *    .start();
 * }</pre>
 */
public final class NetworkServer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(NetworkServer.class.getName());

    private final String name;
    private final int port;
    private final EnvelopeRegistry registry;
    private final EnvelopeAgent handlers = new EnvelopeAgent();
    private final ConnectionListeners listeners = new ConnectionListeners();
    private final ServerConnectionManager connections = new ServerConnectionManager();
    private final PlayerTracker players = new PlayerTracker(connections);
    private final List<EnvelopeCarrier> modules = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private boolean stopped;

    public NetworkServer(int port) {
        this("overseer", port, EnvelopeRegistry.standard());
    }

    /**
     * @param name what servers see as the hub's name
     * @param port port to listen on; 0 picks a free one (see {@link #port()})
     */
    public NetworkServer(String name, int port, EnvelopeRegistry registry) {
        if (port < 0 || port > 65535) throw new IllegalArgumentException("port out of range: " + port);
        this.name = Objects.requireNonNull(name, "name");
        this.port = port;
        this.registry = Objects.requireNonNull(registry, "registry");

        NetworkModule network = new NetworkModule(connections, players);
        install(network).addListener(network);
    }

    public NetworkServer install(EnvelopeCarrier module) {
        handlers.install(module);
        modules.add(module);
        return this;
    }

    public NetworkServer addListener(ConnectionListener listener) {
        listeners.add(listener);
        return this;
    }

    public String name() {
        return name;
    }

    public EnvelopeAgent handlers() {
        return handlers;
    }

    public ServerConnectionManager connections() {
        return connections;
    }

    public PlayerTracker players() {
        return players;
    }

    public EnvelopeRegistry registry() {
        return registry;
    }

    public synchronized NetworkServer start() {
        if (serverChannel != null || stopped)
            throw new IllegalStateException("A NetworkServer can only be started once");
        registry.validate();

        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        EnvelopeEndpoint endpoint = new EnvelopeEndpoint(EnvelopeEndpoint.Role.SERVER, name, null, registry, handlers, new HubListener(), executor);
        try {
            serverChannel = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new EnvelopeChannelInit(endpoint))
                    .bind(port).sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            shutdown();
            throw new GovernorException("Interrupted while binding port $port", e);
        } catch (Exception e) {
            shutdown();
            throw new GovernorException("Could not bind port $port: ${e.getMessage()}", e);
        }
        LOG.log(System.Logger.Level.INFO, "Governor unit '$name()' listening on port ${port()}");
        return this;
    }

    public int port() {
        if (serverChannel == null) throw new IllegalStateException("Not started");
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    public void awaitTermination() {
        Channel channel;
        synchronized (this) {
            channel = serverChannel;
        }
        if (channel != null) channel.closeFuture().syncUninterruptibly();
    }

    public synchronized void shutdown() {
        if (stopped) return;
        stopped = true;
        if (serverChannel != null) serverChannel.close().syncUninterruptibly();
        connections.all().forEach(EnvelopeConnection::close);
        if (bossGroup != null) bossGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        if (workerGroup != null) workerGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        executor.shutdown();
        for (EnvelopeCarrier module : modules) {
            try {
                module.close();
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "Module ${module.getClass().getSimpleName()} failed to close", e);
            }
        }
        LOG.log(System.Logger.Level.INFO, "Overseer hub '$name' stopped");
    }

    @Override
    public void close() {
        shutdown();
    }

    private final class HubListener implements ConnectionListener {

        @Override
        public String handshake(EnvelopeConnection connection, HandshakeRequest handshake) {
            String reason = listeners.handshake(connection, handshake);
            return reason != null ? reason : connections.claim(handshake.name(), handshake.profile(), connection);
        }

        @Override
        public void ready(EnvelopeConnection connection) {
            LOG.log(System.Logger.Level.INFO, connections.of(connection).map(Object::toString).orElse(connection.name())
                    + " connected from ${connection.remote()}");
            listeners.ready(connection);
        }

        @Override
        public void disconnect(EnvelopeConnection connection) {
            if (connection.name() == null) return;
            listeners.disconnect(connection);
            connections.release(connection);
            LOG.log(System.Logger.Level.INFO, "Server '${connection.name()}' disconnected");
        }
    }
}
