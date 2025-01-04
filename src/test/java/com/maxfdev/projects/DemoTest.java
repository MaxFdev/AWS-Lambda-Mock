package com.maxfdev.projects;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.maxfdev.projects.LoggingServer;

/**
 * @author Max Franklin
 * @author Noam Ben Simon
 * @apiNote the HTTPEndpoints for a server's summary and verbose files are as
 *          follows: {@code localhost:[serverPort + 1]/summary} and
 *          {@code localhost:[serverPort + 1]/verbose}
 * @implNote on failure or required cancellation please use {@code pkill java}
 *           in terminal to kill all background JVMs
 */
public class DemoTest {

    private final static int SERVER_COUNT = 7;
    private final static int GATEWAYPORT = 8888;
    private int firstPort = 8000;

    @Test
    public void Demo() {
        // 2. Create a cluster of 7 peer servers and one gateway, starting each in their
        // own JVM.
        System.out.println("---");
        System.out.println("2b - Start the servers on different JVMs:");
        System.out.println("---");
        List<Process> processes = getServers();
        System.out.println("started");

        // 3. Wait until the election has completed before sending any requests to the
        // Gateway. In order to do this, you must add another http based service to the
        // Gateway which can be called to ask if it has a leader or not. If the Gateway
        // has a leader, it should respond with the full list of nodes and their roles
        // (follower vs leader). The script should print out the list of server IDs and
        // their roles.
        System.out.println("---");
        System.out.println("3 - Print the election status:");
        System.out.println("---");
        printStatus();

        // 4. Once the gateway has a leader, send 9 client requests. The script should
        // print out both the request and the response from the cluster. In other words,
        // you wait to get all the responses and print them out. You can either write a
        // client in java or use cURL.
        System.out.println("---");
        System.out.println("4 - Send 9 unique requests:");
        System.out.println("---");
        List<Client> clients = getClients(9, new InetSocketAddress("localhost", GATEWAYPORT), VAR);
        AtomicInteger rq = new AtomicInteger();
        clients.forEach(c -> {
            c.code = VAR.replace("var", String.valueOf(rq.getAndIncrement()));
            c.start();
        });
        clients.forEach(c -> {
            HttpResponse<String> response;
            while ((response = c.getResponse()) == null) {
                Thread.onSpinWait();
            }
            System.out.println("Request:\n" + c.code + "\nResponse:\n" + response.body());
        });

        // 5. kill -9 a follower JVM, printing out which one you are killing. Wait
        // heartbeat interval * 10 time, and then retrieve and display the list of nodes
        // from the Gateway. The dead node should not be on the list.
        // ! note destroy forcibly and kill -9 both use a SIGKILL
        System.out.println("---");
        System.out.println("5 - kill a worker:");
        System.out.println("---");
        processes.get(1).destroyForcibly();
        try {
            System.out.println("killed worker 1");
            System.out.println("waiting for nodes to realize");
            Thread.sleep(45000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        printStatus();

        // 6. kill -9 the leader JVM and then pause 1000 milliseconds. Send/display 9
        // more client requests to the gateway, in the background
        System.out.println("---");
        System.out.println("6 - kill leader:");
        System.out.println("---");
        processes.get(processes.size() - 1).destroyForcibly();
        try {
            System.out.println("waiting a second");
            Thread.sleep(1000);
        } catch (Exception e) {
        }

        System.out.println("send 9 new unique requests");
        clients = getClients(9, new InetSocketAddress("localhost", GATEWAYPORT), VAR);
        clients.forEach(c -> {
            c.code = VAR.replace("var", String.valueOf(rq.getAndIncrement()));
            c.start();
        });

        // 7. Wait for the Gateway to have a new leader, and then print out the node ID
        // of the leader. Print out the responses the client receives from the Gateway
        // for the 9 requests sent in step 6. Do not proceed to step 8 until all 9
        // requests have received responses.
        System.out.println("---");
        System.out.println("7 - Wait for new leader:");
        System.out.println("---");
        printStatus();
        System.out.println("getting earlier responses");
        clients.forEach(c -> {
            HttpResponse<String> response;
            while ((response = c.getResponse()) == null) {
                Thread.onSpinWait();
            }
            System.out.println("Request:\n" + c.code + "\nResponse:\n" + response.body());
        });

        // 8. Send/display 1 more client request (in the foreground), print the
        // response.
        System.out.println("---");
        System.out.println("8 - Send last request:");
        System.out.println("---");
        Client lastClient = getClients(1, new InetSocketAddress("localhost", GATEWAYPORT),
                VAR.replace("var", String.valueOf(rq.getAndIncrement()))).get(0);
        lastClient.start();
        HttpResponse<String> response;
        while ((response = lastClient.getResponse()) == null) {
            Thread.onSpinWait();
        }
        System.out.println("Request:\n" + lastClient.code + "\nResponse:\n" + response.body());

        // 9. List the paths to files containing the Gossip messages received by each
        // node.
        System.out.println("---");
        System.out.println("9 - Listing log paths");
        System.out.println("---");
        try {
            System.out.println("Printing file path to summary logs:");
            ProcessBuilder listSummaryMessages = new ProcessBuilder(
                    "find",
                    ".",
                    "-name",
                    "*summary-Log.txt");
            Process summaries = listSummaryMessages.inheritIO().start();
            summaries.waitFor();
            System.out.println("Printing file path to verbose logs:");
            ProcessBuilder listVerboseMessages = new ProcessBuilder(
                    "find",
                    ".",
                    "-name",
                    "*verbose-Log.txt");
            Process verbose = listVerboseMessages.inheritIO().start();
            verbose.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }

        // 10. Shut down all the nodes
        System.out.println("---");
        System.out.println("10 - Shutting down all JVMs");
        System.out.println("---");
        for (Process process : processes) {
            // Destroy all processes
            process.destroy();
        }
    }

    // ! helper methods:

    private void printStatus() {
        Client client = new Client(0, new InetSocketAddress("localhost", GATEWAYPORT), VAR);
        client.setElectionQuery();
        String results = null;
        while ((results = client.sendElectionQuery()) == null || results.isBlank()) {
            Thread.onSpinWait();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
        System.out.println(results);
    }

    private List<Process> getServers() {
        List<Process> processes = new ArrayList<>();
        try {
            ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(SERVER_COUNT + 1);
            for (long i = 1; i <= SERVER_COUNT; i++) {
                peerIDtoAddress.put(i, new InetSocketAddress("localhost", getNextPort()));
            }
            int gatewayPort = getNextPort();
            long gatewayID = SERVER_COUNT + 1;
            // Spawn gateway in new JVM
            ProcessBuilder gwBuilder = new ProcessBuilder(
                    "java",
                    "-cp",
                    System.getProperty("java.class.path"),
                    "edu.yu.cs.com3800.stage5.DemoTest$DemoGatewayServer",
                    String.valueOf(GATEWAYPORT), // int httpPort
                    String.valueOf(gatewayPort), // int peerPort
                    "0", // long peerEpoch
                    String.valueOf(gatewayID), // Long serverID
                    "1", // int numberOfObservers
                    String.valueOf(peerIDtoAddress.size())); // int peerIDtoAddressSize
            for (Map.Entry<Long, InetSocketAddress> e : peerIDtoAddress.entrySet()) {
                gwBuilder.command().add(String.valueOf(e.getKey())); // long peerID
                gwBuilder.command().add(String.valueOf(e.getValue().getPort())); // int peerUDPPort
            }
            Process gwProcess = gwBuilder.inheritIO().start();
            System.out.println("ID: " + gatewayID + " on pid: " + gwProcess.pid());
            processes.add(gwProcess);
            peerIDtoAddress.put(gatewayID, new InetSocketAddress("localhost", gatewayPort));
            // Spawn peers in new JVM
            for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
                if (entry.getKey() != gatewayID) {
                    ConcurrentHashMap<Long, InetSocketAddress> peerMap = new ConcurrentHashMap<>(peerIDtoAddress);
                    peerMap.remove(entry.getKey());
                    ProcessBuilder pb = new ProcessBuilder(
                            "java",
                            "-cp",
                            System.getProperty("java.class.path"),
                            "edu.yu.cs.com3800.stage5.DemoTest$DemoPeerServer",
                            String.valueOf(entry.getValue().getPort()), // int udpPort
                            "0", // long peerEpoch
                            String.valueOf(entry.getKey()), // Long serverID
                            String.valueOf(gatewayID), // Long gatewayID
                            "1", // int numberOfObservers
                            String.valueOf(peerMap.size())); // int peerIDtoAddressSize
                    for (Map.Entry<Long, InetSocketAddress> e : peerMap.entrySet()) {
                        pb.command().add(String.valueOf(e.getKey())); // long peerID
                        pb.command().add(String.valueOf(e.getValue().getPort())); // int peerUDPPort
                    }
                    Process process = pb.inheritIO().start();
                    System.out.println("ID: " + entry.getKey() + " on pid: " + process.pid());
                    processes.add(process);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            assert false;
        }
        return processes;
    }

    public List<Client> getClients(int amount, InetSocketAddress gatewayAddress, String code) {
        List<Client> clients = new ArrayList<Client>(amount);
        for (int i = 0; i < amount; i++) {
            clients.add(new Client(i, gatewayAddress, code));
        }
        return clients;
    }

    private int getNextPort() {
        int port = this.firstPort;
        this.firstPort += 4;
        return port;
    }

    @BeforeAll
    public static void cleanUpLogs() {
        File stageDir = new File("./");
        if (stageDir.exists() && stageDir.isDirectory()) {
            List<File> dirs = new ArrayList<>();
            for (File entry : stageDir.listFiles()) {
                if (entry.getName().startsWith("logs")) {
                    dirs.add(entry);
                }
            }
            Collections.sort(dirs, (o1, o2) -> o2.getName().compareTo(o1.getName()));
            while (dirs.size() > 0) {
                deleteDir(dirs.remove(dirs.size() - 1));
            }
        }
    }

    private static void deleteDir(File dir) {
        File[] content = dir.listFiles();
        for (File file : content) {
            if (file.isDirectory()) {
                deleteDir(file);
            }
            file.delete();
        }
        dir.delete();
    }

    // ! helper classes

    private static class DemoPeerServer {
        public static void main(String[] args) throws IOException {
            // get the params
            int udpPort = Integer.parseInt(args[0]);
            long peerEpoch = Long.parseLong(args[1]);
            Long serverID = Long.parseLong(args[2]);
            Long gatewayID = Long.parseLong(args[3]);
            int numberOfObservers = Integer.parseInt(args[4]);

            // assemble the map
            int amountOfPeers = Integer.parseInt(args[5]);
            Map<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(amountOfPeers);
            for (int i = 6; i < args.length - 1; i += 2) {
                long id = Long.parseLong(args[i]);
                InetSocketAddress address = new InetSocketAddress("localhost", Integer.parseInt(args[i + 1]));
                peerIDtoAddress.put(id, address);
            }

            // create and run peer server
            PeerServerImpl peerServerImpl = new PeerServerImpl(
                    udpPort,
                    peerEpoch,
                    serverID,
                    peerIDtoAddress,
                    gatewayID,
                    numberOfObservers);
            peerServerImpl.start();
        }
    }

    private static class DemoGatewayServer {
        public static void main(String[] args) throws IOException {
            // get the params
            int httpPort = Integer.parseInt(args[0]);
            int peerPort = Integer.parseInt(args[1]);
            long peerEpoch = Long.parseLong(args[2]);
            Long serverID = Long.parseLong(args[3]);
            int numberOfObservers = Integer.parseInt(args[4]);

            // assemble the map
            int amountOfPeers = Integer.parseInt(args[5]);
            ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(amountOfPeers);
            for (int i = 6; i < args.length - 1; i += 2) {
                long id = Long.parseLong(args[i]);
                InetSocketAddress address = new InetSocketAddress("localhost", Integer.parseInt(args[i + 1]));
                peerIDtoAddress.put(id, address);
            }

            // create and run gateway
            GatewayServer gatewayServer = new GatewayServer(
                    httpPort,
                    peerPort,
                    peerEpoch,
                    serverID,
                    peerIDtoAddress,
                    numberOfObservers);
            gatewayServer.start();
        }
    }

    private class Client extends Thread implements LoggingServer {

        private final String HEADER_NAME = "Content-Type";
        private final String HEADER_VALUE = "text/x-java-source";

        private final Logger logger;
        private String code;
        private final LinkedBlockingQueue<HttpResponse<String>> responses;
        private URI uri;
        private HttpRequest request;

        private Client(int id, InetSocketAddress gatewayAddress, String code) {
            try {
                this.logger = initializeLogging("Client-" + id, true);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            this.logger.info("Constructing client");
            this.code = new String(code);
            this.responses = new LinkedBlockingQueue<>();
            try {
                this.uri = new URI("http", null, gatewayAddress.getHostName(), gatewayAddress.getPort(),
                        "/compileandrun", null, null);
            } catch (URISyntaxException e) {
                throw new IllegalStateException(e);
            }
        }

        private void setElectionQuery() {
            try {
                this.uri = new URI("http", null, "localhost", GATEWAYPORT, "/electionstatus", null, null);
            } catch (URISyntaxException e) {
            }
        }

        private String sendElectionQuery() {
            try {
                HttpRequest req = HttpRequest.newBuilder().uri(this.uri).GET().build();
                return HttpClient.newHttpClient().send(req, BodyHandlers.ofString()).body();
            } catch (Exception e) {
                this.logger.severe("Error sending:\n" + e.getLocalizedMessage());
                System.err.println(e.getLocalizedMessage());
                return null;
            }
        }

        private void sendCode() {
            this.logger.info("sending request:\n" + this.code);

            try {
                HttpClient client = HttpClient.newHttpClient();
                if (this.request == null) {
                    this.request = HttpRequest.newBuilder(this.uri).header(HEADER_NAME, HEADER_VALUE)
                            .POST(BodyPublishers.ofString(this.code)).build();
                }
                this.responses.put(client.send(this.request, BodyHandlers.ofString()));
            } catch (Exception e) {
                this.logger.severe("Error sending:\n" + e.getLocalizedMessage());
                System.err.println(e.getLocalizedMessage());
            }
        }

        private HttpResponse<String> getResponse() {
            try {
                HttpResponse<String> response = this.responses.poll(20, TimeUnit.MILLISECONDS);
                if (response != null) {
                    this.logger.info("response received");
                }
                return response;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public void run() {
            this.logger.info("running");
            sendCode();
            this.logger.info("done");
        }
    }

    private final static String VAR = "public class Variable { public String run() { return \"var\"; } }";
}
