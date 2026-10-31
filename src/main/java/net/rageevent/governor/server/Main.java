package net.rageevent.governor.server;

import net.rageevent.governor.server.modules.QueueModule;
import net.rageevent.governor.server.modules.SystemModule;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;

public class Main {

    public static final int DEFAULT_PORT = 25570;
    private static final Duration WAIT = Duration.ofSeconds(5);

    static void main(String[] args) {

        int port = DEFAULT_PORT;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port")) port = Integer.parseInt(args[i + 1]);
        }

        NetworkServer hub = new NetworkServer(port);
        hub.install(new SystemModule()).install(new QueueModule(hub)).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> readTerminalInput(hub), "Overseer-Shutdown"));
        hub.awaitTermination();
    }

    private static void readTerminalInput(NetworkServer hub){
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Terminal ready. Type 'shutdown' and press Enter to stop.");

        while (true) {
            String input;
            try {
                input = reader.readLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (input == null) break;
            if ("shutdown".equalsIgnoreCase(input.trim())) {
                System.out.println("[Terminal Command Received] Initiating graceful shutdown...");
                hub.shutdown();
                break;
            } else System.out.println("Unknown terminal command. Type 'shutdown' to stop.");
        }
    }

}
