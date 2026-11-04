package net.rageevent.governor.server.players;

import net.rageevent.governor.api.UserNotice;
import net.rageevent.governor.api.exceptions.GovernorException;
import net.rageevent.governor.api.shipments.network.player.ProxyRoster;
import net.rageevent.governor.api.shipments.network.player.TransferPlayerRequest;
import net.rageevent.governor.api.shipments.network.player.TransferResult;
import net.rageevent.governor.server.ConnectedServer;
import net.rageevent.governor.server.ServerConnectionManager;
import net.rageevent.myriad.server.ServerModeType;
import net.rageevent.myriad.server.ServerType;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public final class PlayerTracker {

    private static final System.Logger LOG = System.getLogger(PlayerTracker.class.getName());

    static final Duration TRANSFER_TIMEOUT = Duration.ofSeconds(15);

    private record PendingTransfer(String server, long expiresAt) {
        boolean expired() {
            return System.nanoTime() - expiresAt > 0;
        }
    }

    private final ServerConnectionManager servers;
    private final Map<UUID, OnlinePlayer> online = new ConcurrentHashMap<>();
    private final Map<String, UUID> onlineByName = new ConcurrentHashMap<>();
    private final Map<UUID, String> knownNames = new ConcurrentHashMap<>();
    private final Map<String, UUID> knownByName = new ConcurrentHashMap<>();
    private final Map<String, Integer> serverCounts = new ConcurrentHashMap<>();
    private final Map<UUID, PendingTransfer> pendingTransfers = new ConcurrentHashMap<>();
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    public PlayerTracker(ServerConnectionManager servers) {
        this.servers = servers;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }


    public Optional<OnlinePlayer> find(UUID player) {
        return player == null ? Optional.empty() : Optional.ofNullable(online.get(player));
    }

    public Optional<OnlinePlayer> find(String name) {
        return name == null ? Optional.empty() : find(onlineByName.get(key(name)));
    }

    public Optional<UUID> resolve(String name) {
        if (name == null) return Optional.empty();
        UUID uuid = onlineByName.get(key(name));
        return Optional.ofNullable(uuid != null ? uuid : knownByName.get(key(name)));
    }

    public Optional<String> nameOf(UUID player) {
        OnlinePlayer current = online.get(player);
        return Optional.ofNullable(current != null ? current.name() : knownNames.get(player));
    }

    public boolean isOnline(UUID player) {
        return online.containsKey(player);
    }

    public Collection<OnlinePlayer> all() {
        return List.copyOf(online.values());
    }

    public int count() {
        return online.size();
    }

    public int playersOn(String server) {
        return serverCounts.getOrDefault(server, 0);
    }

    public int load(String server) {
        int inbound = 0;
        for (Map.Entry<UUID, PendingTransfer> entry : pendingTransfers.entrySet()) {
            if (entry.getValue().expired()) {
                pendingTransfers.remove(entry.getKey(), entry.getValue());
            } else if (entry.getValue().server().equals(server)) {
                inbound++;
            }
        }
        return playersOn(server) + inbound;
    }

    public int freeSlots(ConnectedServer server) {
        if (server.maxPlayers() <= 0) return Integer.MAX_VALUE;
        return Math.max(0, server.maxPlayers() - load(server.name()));
    }

    /**
     * Open backends of a serverType that still have room.
     */
    public List<ConnectedServer> joinable(ServerType group) {
        return servers.inGroup(group).stream()
                .filter(server -> server.state().joinable() && freeSlots(server) > 0)
                .toList();
    }

    public List<ConnectedServer> joinable(ServerModeType group) {
        return servers.inMode(group).stream()
                .filter(server -> server.state().joinable() && freeSlots(server) > 0)
                .toList();
    }

    public boolean notify(UserNotice notice) {
        OnlinePlayer player = online.get(notice.player());
        if (player == null) return false;
        Optional<ConnectedServer> proxy = servers.get(player.proxy());
        proxy.ifPresent(connection -> connection.send(notice));
        return proxy.isPresent();
    }

    public CompletableFuture<TransferResult> transfer(UUID uuid, String server) {
        OnlinePlayer player = online.get(uuid);
        if (player == null) {
            return done(TransferResult.Status.NOT_ONLINE, server, "Player is not online");
        }
        Optional<ConnectedServer> target = servers.get(server).filter(ConnectedServer::isGameServer);
        if (target.isEmpty()) {
            return done(TransferResult.Status.UNKNOWN_SERVER, server, "No server named $server is connected");
        }
        if (server.equals(player.server())) {
            return done(TransferResult.Status.ALREADY_CONNECTED, server, null);
        }
        Optional<ConnectedServer> proxy = servers.get(player.proxy());
        if (proxy.isEmpty()) {
            return done(TransferResult.Status.FAILED, server, "Proxy ${player.proxy()} is not connected");
        }

        PendingTransfer reservation = new PendingTransfer(server, System.nanoTime() + TRANSFER_TIMEOUT.toNanos());
        pendingTransfers.put(uuid, reservation);
        return proxy.get().request(new TransferPlayerRequest(uuid, server), TRANSFER_TIMEOUT)
                .handle((result, error) -> {
                    TransferResult outcome = error != null
                            ? new TransferResult(TransferResult.Status.FAILED, server, GovernorException.unwrap(error).getMessage())
                            : result;
                    if (outcome.status() != TransferResult.Status.SUCCESS) pendingTransfers.remove(uuid, reservation);
                    return outcome;
                });
    }

    public CompletableFuture<TransferResult> sendToGroup(UUID uuid, ServerType group) {
        OnlinePlayer player = online.get(uuid);
        if (player == null) {
            return done(TransferResult.Status.NOT_ONLINE, null, "Player is not online");
        }
        Optional<ConnectedServer> best = joinable(group).stream()
                .filter(server -> !server.name().equals(player.server()))
                .min(Comparator.comparingInt(server -> load(server.name())));
        if (best.isPresent()) return transfer(uuid, best.get().name());

        boolean alreadyThere = servers.get(player.server()).map(server -> server.isGameServer() && server.serverType() == group).orElse(false);
        return alreadyThere
                ? done(TransferResult.Status.ALREADY_CONNECTED, player.server(), null)
                : done(TransferResult.Status.NO_SERVER_AVAILABLE, null, "No open $group server has room");
    }

    private static CompletableFuture<TransferResult> done(TransferResult.Status status, String server, String detail) {
        return CompletableFuture.completedFuture(new TransferResult(status, server, detail));
    }

    public void join(String proxy, UUID uuid, String name, String server) {
        OnlinePlayer before;
        OnlinePlayer after = new OnlinePlayer(uuid, name, proxy, server);
        synchronized (this) {
            before = online.get(uuid);
            put(before, after);
        }
        if (before == null) {
            fire(listener -> listener.onJoin(after));
        } else if (!Objects.equals(before.server(), server)) {
            fire(listener -> listener.onSwitch(before, after));
        }
    }

    public void switchServer(String proxy, UUID uuid, String server) {
        OnlinePlayer before;
        OnlinePlayer after;
        synchronized (this) {
            before = online.get(uuid);
            if (before == null || !before.proxy().equals(proxy) || Objects.equals(before.server(), server)) return;
            after = before.withServer(server);
            put(before, after);
        }
        fire(listener -> listener.onSwitch(before, after));
    }

    public void leave(String proxy, UUID uuid) {
        OnlinePlayer gone;
        synchronized (this) {
            gone = online.get(uuid);
            if (gone == null || !gone.proxy().equals(proxy)) return;
            remove(gone);
        }
        fire(listener -> listener.onLeave(gone));
    }

    public void replaceRoster(String proxy, List<ProxyRoster.UserEntry> roster) {
        Set<UUID> present = new HashSet<>();
        roster.forEach(entry -> present.add(entry.player()));
        List<OnlinePlayer> gone = new ArrayList<>();
        synchronized (this) {
            for (OnlinePlayer player : online.values()) {
                if (player.proxy().equals(proxy) && !present.contains(player.uuid())) gone.add(player);
            }
            gone.forEach(this::remove);
        }
        gone.forEach(player -> fire(listener -> listener.onLeave(player)));
        roster.forEach(entry -> join(proxy, entry.player(), entry.name(), entry.server()));
    }

    public void proxyLost(String proxy) {
        replaceRoster(proxy, List.of());
    }

    private void put(OnlinePlayer before, OnlinePlayer after) {
        if (before != null) {
            onlineByName.remove(key(before.name()), before.uuid());
            decrement(before.server());
        }
        online.put(after.uuid(), after);
        onlineByName.put(key(after.name()), after.uuid());
        increment(after.server());

        String oldName = knownNames.put(after.uuid(), after.name());
        if (oldName != null && !oldName.equalsIgnoreCase(after.name())) knownByName.remove(key(oldName), after.uuid());
        knownByName.put(key(after.name()), after.uuid());

        PendingTransfer pending = pendingTransfers.get(after.uuid());
        if (pending != null && pending.server().equals(after.server())) pendingTransfers.remove(after.uuid(), pending);
    }

    private void remove(OnlinePlayer player) {
        online.remove(player.uuid());
        onlineByName.remove(key(player.name()), player.uuid());
        decrement(player.server());
        pendingTransfers.remove(player.uuid());
    }

    private void increment(String server) {
        if (server != null) serverCounts.merge(server, 1, Integer::sum);
    }

    private void decrement(String server) {
        if (server != null) serverCounts.computeIfPresent(server, (_, count) -> count <= 1 ? null : count - 1);
    }

    private void fire(Consumer<Listener> event) {
        for (Listener listener : listeners) {
            try {
                event.accept(listener);
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "Player listener " + listener.getClass().getSimpleName() + " failed", e);
            }
        }
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }


    public interface Listener {
        default void onJoin(OnlinePlayer player) {
        }

        default void onSwitch(OnlinePlayer before, OnlinePlayer after) {
        }

        default void onLeave(OnlinePlayer player) {
        }
    }
}
