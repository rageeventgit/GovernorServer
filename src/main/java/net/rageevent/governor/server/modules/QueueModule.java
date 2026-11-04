package net.rageevent.governor.server.modules;

import net.rageevent.governor.api.EnvelopeAgent;
import net.rageevent.governor.api.EnvelopeCarrier;
import net.rageevent.governor.api.EnvelopeConnection;
import net.rageevent.governor.api.shipments.network.player.TransferResult;
import net.rageevent.governor.api.shipments.queue.*;
import net.rageevent.governor.server.ConnectedServer;
import net.rageevent.governor.server.NetworkServer;
import net.rageevent.governor.server.ServerConnectionManager;
import net.rageevent.governor.server.players.OnlinePlayer;
import net.rageevent.governor.server.players.PlayerTracker;
import net.rageevent.myriad.server.ServerModeType;
import net.rageevent.myriad.server.ServerModeType;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class QueueModule implements EnvelopeCarrier, PlayerTracker.Listener {

    private static final System.Logger LOG = System.getLogger(QueueModule.class.getName());

    static final Duration FAILURE_COOLDOWN = Duration.ofSeconds(10);

    private final ServerConnectionManager servers;
    private final PlayerTracker players;
    private final Map<ServerModeType, Deque<UUID>> queues = new HashMap<>();
    private final Map<UUID, ServerModeType> queueOf = new HashMap<>();
    private final Map<UUID, Integer> lastAnnounced = new HashMap<>();
    private final Map<String, Long> cooldownUntil = new HashMap<>();
    private final ScheduledExecutorService ticker;

    public QueueModule(NetworkServer hub) {
        this(hub, Duration.ofSeconds(1));
    }

    public QueueModule(NetworkServer hub, Duration tickInterval) {
        this.servers = hub.connections();
        this.players = hub.players();
        this.ticker = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("Overseer-Queues").daemon().factory());
        ticker.scheduleWithFixedDelay(this::tick, tickInterval.toMillis(), tickInterval.toMillis(), TimeUnit.MILLISECONDS);
        players.addListener(this);
    }

    @Override
    public void register(EnvelopeAgent handlers) {
        handlers.onTransaction(JoinQueueRequest.class, this::join)
                .onTransaction(LeaveQueueRequest.class, (server, request) -> leave(request.player()));
    }

    @Override
    public void close() {
        ticker.shutdownNow();
    }

    public synchronized List<UUID> waiting(String queue) {
        return List.copyOf(queues.getOrDefault(key(queue), new ArrayDeque<>()));
    }

    private QueueResult join(EnvelopeConnection origin, JoinQueueRequest request) {
        UUID player = request.player();
        ServerModeType queue = request.queue();
        if (!players.isOnline(player)) return new QueueResult(QueueResult.Status.NOT_ONLINE, queue, 0, 0);
        if (servers.inMode(queue).isEmpty()) return new QueueResult(QueueResult.Status.UNKNOWN_QUEUE, queue, 0, 0);

        QueueResult result;
        synchronized (this) {
            Deque<UUID> line = queues.computeIfAbsent(queue, name -> new ArrayDeque<>());
            if (queue.equals(queueOf.get(player))) {
                return new QueueResult(QueueResult.Status.ALREADY_QUEUED, queue, positionOf(line, player), line.size());
            }
            removeFromQueue(player);
            line.addLast(player);
            queueOf.put(player, queue);
            lastAnnounced.put(player, line.size());
            result = new QueueResult(QueueResult.Status.JOINED, queue, line.size(), line.size());
        }

        ticker.execute(this::tick);
        return result;
    }

    private synchronized QueueResult leave(UUID player) {
        ServerModeType queue = queueOf.get(player);
        if (queue == null) return new QueueResult(QueueResult.Status.NOT_QUEUED, null, 0, 0);
        removeFromQueue(player);
        return new QueueResult(QueueResult.Status.LEFT, queue, 0, queues.getOrDefault(queue, new ArrayDeque<>()).size());
    }

    private void tick() {
        try {
            List<Runnable> dispatches = new ArrayList<>();
            synchronized (this) {
                for (Map.Entry<ServerModeType, Deque<UUID>> entry : queues.entrySet()) {
                    match(entry.getKey(), entry.getValue(), dispatches);
                }
                queues.values().removeIf(Deque::isEmpty);
                announcePositions();
            }
            dispatches.forEach(Runnable::run);
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "Queue tick failed", e);
        }
    }

    private void match(ServerModeType queue, Deque<UUID> line, List<Runnable> dispatches) {
        if (line.isEmpty()) return;
        long now = System.nanoTime();
        cooldownUntil.values().removeIf(until -> now - until > 0);
        List<ConnectedServer> open = new ArrayList<>(players.joinable(queue));
        open.removeIf(server -> cooldownUntil.containsKey(server.name()));

        open.sort(Comparator.comparingInt(players::freeSlots));
        for (ConnectedServer server : open) {
            int free = players.freeSlots(server);
            while (free > 0 && !line.isEmpty()) {
                UUID player = line.pollFirst();
                queueOf.remove(player);
                lastAnnounced.remove(player);
                free--;
                dispatches.add(() -> send(player, queue, server.name()));
            }
        }
    }

    private void send(UUID player, ServerModeType queue, String server) {
        players.notify(new QueueMatched(player, queue, server));
        players.transfer(player, server).thenAccept(result -> {
            if (result.status() == TransferResult.Status.SUCCESS || result.status() == TransferResult.Status.ALREADY_CONNECTED)
                return;
            if (result.status() == TransferResult.Status.NOT_ONLINE) return;
            LOG.log(System.Logger.Level.INFO, "Sending $player to $server failed (${result.status()}: ${result.detail()}); " +
                    "back to the front of $queue, $server skipped for ${FAILURE_COOLDOWN.toSeconds()} s");
            requeueAtFront(player, queue, server);
        });
    }

    private synchronized void requeueAtFront(UUID player, ServerModeType queue, String failedServer) {
        cooldownUntil.put(failedServer, System.nanoTime() + FAILURE_COOLDOWN.toNanos());
        if (queueOf.containsKey(player) || !players.isOnline(player)) return;
        queues.computeIfAbsent(queue, name -> new ArrayDeque<>()).addFirst(player);
        queueOf.put(player, queue);
    }

    private void announcePositions() {
        queues.forEach((queue, line) -> {
            int position = 0;
            for (UUID player : line) {
                position++;
                Integer previous = lastAnnounced.put(player, position);
                if (previous == null || previous != position) {
                    players.notify(new QueueUpdate(player, queue, position, line.size()));
                }
            }
        });
    }

    private void removeFromQueue(UUID player) {
        ServerModeType queue = queueOf.remove(player);
        lastAnnounced.remove(player);
        if (queue == null) return;
        Deque<UUID> line = queues.get(queue);
        if (line != null) line.remove(player);
    }

    private static int positionOf(Deque<UUID> line, UUID player) {
        int position = 0;
        for (UUID uuid : line) {
            position++;
            if (uuid.equals(player)) return position;
        }
        return 0;
    }

    @Override
    public synchronized void onLeave(OnlinePlayer player) {
        removeFromQueue(player.uuid());
    }

    private static String key(String queue) {
        return queue == null ? "" : queue.toLowerCase(Locale.ROOT);
    }
}
