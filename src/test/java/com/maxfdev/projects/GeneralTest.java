package com.maxfdev.projects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.maxfdev.projects.LoggingServer;
import com.maxfdev.projects.PeerServer;
import com.maxfdev.projects.Vote;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GeneralTest {

    @Test
    @Order(1)
    public void basicTest() {
        // set up servers
        constructSystem(2);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);

        // set up a client
        Client client = getClients(1, getGatewayAddress(servers), HELLO).get(0);
        client.start();

        // get response
        HttpResponse<String> response;
        long startTime = System.currentTimeMillis();
        while ((response = client.getResponse()) == null &&
                System.currentTimeMillis() - startTime < 5000) {
            Thread.onSpinWait();
        }

        // check results
        assertTrue(response != null, "Client response timeout");
        printMessage(response);
        assertEquals("Hello", response.body(), "Bad server response");
    }

    @Test
    @Order(2)
    public void cacheTest() {
        constructSystem(4);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        List<Client> clients = getClients(6, getGatewayAddress(servers), HELLO);
        int misses = 0;
        for (int i = 0; i < clients.size(); i++) {
            clients.get(i).start();
            if (i == 0) {
                Client client = clients.get(i);
                HttpResponse<String> response;
                long startTime = System.currentTimeMillis();
                while ((response = client.getResponse()) == null && System.currentTimeMillis() - startTime < 5000) {
                    Thread.onSpinWait();
                }
                assertTrue(response != null, "Client response timeout");
                if (response.headers().firstValue("Cached-Response").get().equals("false")) {
                    misses++;
                }
                printMessage(response);
                assertEquals("Hello", response.body(), "Bad server response");
            }
        }
        for (int i = 1; i < clients.size(); i++) {
            Client client = clients.get(i);
            HttpResponse<String> response;
            long startTime = System.currentTimeMillis();
            while ((response = client.getResponse()) == null && System.currentTimeMillis() - startTime < 5000) {
                Thread.onSpinWait();
            }
            assertTrue(response != null, "Client response timeout");
            if (response.headers().firstValue("Cached-Response").get().equals("false")) {
                misses++;
            }
            printMessage(response);
            assertEquals("Hello", response.body(), "Bad server response");
        }
        assertEquals(1, misses, "Too many misses");
    }

    @Test
    @Order(3)
    public void nullReturnTest() {
        constructSystem(3);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        Client client = getClients(1, getGatewayAddress(servers), NULL).get(0);
        client.start();
        HttpResponse<String> response;
        long timer = System.currentTimeMillis();
        while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 5000) {
            Thread.onSpinWait();
        }
        assertTrue(response != null, "Client response timeout");
        printMessage(response);
        assertEquals("null", response.body(), "Bad server response");
    }

    @Test
    @Order(4)
    public void compilationErrorTest() {
        constructSystem(2);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        Client client = getClients(1, getGatewayAddress(servers), ERR).get(0);
        client.start();
        HttpResponse<String> response;
        long timer = System.currentTimeMillis();
        while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 5000) {
            Thread.onSpinWait();
        }
        assertTrue(response != null, "Client response timeout");
        printMessage(response);
        assertTrue(response.body().startsWith("Code did not compile"), "Bad server response");
    }

    @Test
    @Order(5)
    public void runtimeError() {
        constructSystem(2);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        Client client = getClients(1, getGatewayAddress(servers), THROW).get(0);
        client.start();
        HttpResponse<String> response;
        long timer = System.currentTimeMillis();
        while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 5000) {
            Thread.onSpinWait();
        }
        assertTrue(response != null, "Client response timeout");
        printMessage(response);
        assertTrue(response.body().startsWith(""), "Bad server response");
    }

    @Test
    @Order(6)
    public void parallelUncachedRunTest() {
        constructSystem(6);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        List<Client> clients = getClients(10, getGatewayAddress(servers), RUN);
        for (int i = 0; i < clients.size(); i++) {
            Client client = clients.get(i);
            client.code = client.code.replace("running\"", "running for client " + i + "\"");
            client.start();
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
        for (Client client : clients) {
            HttpResponse<String> response;
            long timer = System.currentTimeMillis();
            while ((response = client.getResponse()) == null) {
                if (System.currentTimeMillis() - timer > 5000) {
                    break;
                }
                Thread.onSpinWait();
            }
            assertTrue(response != null, "Client response timeout");
            printMessage(response);
            assertTrue(response.body().startsWith("printed running"), "Bad server response");

        }
    }

    @Test
    @Order(7)
    public void manyClientsTest() {
        constructSystem(5);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        List<Client> clients = getClients(10, getGatewayAddress(servers), HELLO);
        for (Client client : clients) {
            client.start();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
            }
        }
        for (Client client : clients) {
            HttpResponse<String> response;
            long timer = System.currentTimeMillis();
            while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 5000) {
                Thread.onSpinWait();
            }
            assertTrue(response != null, "Client response timeout");
            printMessage(response);
            assertEquals("Hello", response.body(), "Bad server response");

        }
    }

    @Test
    @Order(8)
    public void manyClientsRandomTest() {
        constructSystem(5);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        List<Client> clients = getClients(20, getGatewayAddress(servers), HELLO);
        String[] codes = { ERR, HELLO, NULL, RUN, THROW, VAR, WORK10 };
        Random random = new Random();
        for (Client client : clients) {
            client.code = new String(codes[random.nextInt(0, codes.length)]);
            client.start();
        }
        for (Client client : clients) {
            HttpResponse<String> response;
            long timer = System.currentTimeMillis();
            while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 10000) {
                Thread.onSpinWait();
            }
            assertTrue(response != null, "Client response timeout");
            printMessage(response);
        }
    }

    @Test
    @Order(9)
    public void singleWorkerTest() {
        constructSystem(2);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        List<Client> clients = getClients(10, getGatewayAddress(servers), VAR);
        for (int i = 0; i < clients.size(); i++) {
            Client client = clients.get(i);
            client.code = client.code.replace("\"var\"", "\"Client " + i + " work processed\"");
            client.start();
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
            }
        }
        for (Client client : clients) {
            HttpResponse<String> response;
            long timer = System.currentTimeMillis();
            while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 5000) {
                Thread.onSpinWait();
            }
            assertTrue(response != null, "Client response timeout");
            printMessage(response);
            assertTrue(response.body().startsWith("Client "), "Bad server response");
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
            }
        }
    }

    @Test
    @Order(10)
    public void observerWithHigherID() {
        constructSystem(3);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        Client client = getClients(1, getGatewayAddress(servers), HELLO).get(0);
        client.start();
        HttpResponse<String> response;
        long timer = System.currentTimeMillis();
        while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 5000) {
            Thread.onSpinWait();
        }
        assertTrue(response != null, "Client response timeout");
        printMessage(response);
        assertEquals("Hello", response.body(), "Bad server response");
    }

    @Test
    @Order(11)
    public void badContextTest() {
        constructSystem(2);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        Client client = getClients(1, getGatewayAddress(servers), HELLO).get(0);
        client.setBadContext();
        client.start();
        HttpResponse<String> response;
        long timer = System.currentTimeMillis();
        while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 5000) {
            Thread.onSpinWait();
        }
        assertTrue(response != null, "Client response timeout");
        printMessage(response);
        assertEquals(404, response.statusCode(), "Bad server response");
    }

    @Test
    @Order(12)
    public void badMethodTest() {
        constructSystem(2);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        Client client = getClients(1, getGatewayAddress(servers), HELLO).get(0);
        client.setBadMethod();
        client.start();
        HttpResponse<String> response;
        long timer = System.currentTimeMillis();
        while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 5000) {
            Thread.onSpinWait();
        }
        assertTrue(response != null, "Client response timeout");
        printMessage(response);
        assertEquals(405, response.statusCode(), "Bad server response");
    }

    @Test
    @Order(13)
    public void badHeadersTest() {
        constructSystem(2);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        Client client = getClients(1, getGatewayAddress(servers), HELLO).get(0);
        client.setBadHeaders();
        client.start();
        HttpResponse<String> response;
        long timer = System.currentTimeMillis();
        while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 5000) {
            Thread.onSpinWait();
        }
        assertTrue(response != null, "Client response timeout");
        printMessage(response);
        assertEquals(400, response.statusCode(), "Bad server response");
    }

    @Test
    @Order(14)
    public void stressTest() {
        constructSystem(6);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);
        List<Client> clients = getClients(100, getGatewayAddress(servers), VAR);
        String[] codes = { ERR, HELLO, NULL, RUN, THROW, VAR, WORK10 };
        Random random = new Random();
        for (int i = 0; i < clients.size(); i++) {
            Client client = clients.get(i);
            if (i % 5 == 0) {
                client.code = new String(codes[random.nextInt(0, codes.length)]);
            } else {
                client.code = client.code.replace("\"var\"", "\"Client " + i + " work processed\"");
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                }
            }
            client.start();
        }
        for (Client client : clients) {
            HttpResponse<String> response;
            long timer = System.currentTimeMillis();
            while ((response = client.getResponse()) == null && System.currentTimeMillis() - timer < 15000) {
                Thread.onSpinWait();
            }
            assertTrue(response != null, "Client response timeout");
            printMessage(response);
        }
    }

    @Test
    @Order(15)
    public void basicGossipTest() {
        // set up servers
        constructSystem(2);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);

        // set up a client
        List<Client> clients = getClients(5, getGatewayAddress(servers), WORK10);
        for (int i = 0; i < clients.size(); i++) {
            Client client = clients.get(i);
            client.code = client.code.replace("10", String.valueOf(5000 + i));
            client.start();
        }

        // get response
        for (int i = 0; i < clients.size(); i++) {
            Client client = clients.get(i);
            HttpResponse<String> response;
            long startTime = System.currentTimeMillis();
            while ((response = client.getResponse()) == null &&
                    System.currentTimeMillis() - startTime < 30000) {
                Thread.onSpinWait();
            }

            // check results
            assertTrue(response != null, "Client response timeout");
            printMessage(response);
            assertTrue(response.body().startsWith("Work done for time: "), "Bad server response");
        }
    }

    @Test
    @Order(16)
    public void workerFailureTest() {
        // set up servers
        constructSystem(3);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);

        // set up a client
        List<Client> clients = getClients(5, getGatewayAddress(servers), WORK10);
        for (int i = 0; i < clients.size(); i++) {
            Client client = clients.get(i);
            client.code = client.code.replace("10", String.valueOf(5000 + i));
            client.start();
        }

        // kill worker 1
        try {
            Thread.sleep(3500);
        } catch (InterruptedException e) {
        }
        ((PeerServerImpl) servers.get(0)).shutdown();
        System.out.println("Server 1 shutdown");

        // get response
        for (int i = 0; i < clients.size(); i++) {
            Client client = clients.get(i);
            HttpResponse<String> response;
            long startTime = System.currentTimeMillis();
            while ((response = client.getResponse()) == null &&
                    System.currentTimeMillis() - startTime < 45000) {
                Thread.onSpinWait();
            }

            // check results
            assertTrue(response != null, "Client response timeout");
            printMessage(response);
            assertTrue(response.body().startsWith("Work done for time: "), "Bad server response");
        }
    }

    // TODO figure out why responses are making it throw after shutdown
    @Test
    @Order(17)
    public void leaderFailureTest() {
        // set up servers
        constructSystem(3);
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServer> servers = getServers();
        servers.add(gatewayServer.getPeerServer());
        printServersLeader(servers);

        // wait for heartbeats to register
        try {
            Thread.sleep(3500);
        } catch (InterruptedException e) {
        }

        // set up a client
        List<Client> clients = getClients(5, getGatewayAddress(servers), WORK10);
        for (int i = 0; i < clients.size(); i++) {
            Client client = clients.get(i);
            client.code = client.code.replace("10", String.valueOf(6000 + i));
            client.start();

            if (i == 0) {
                // kill leader
                int leaderIndex = servers.size() - 2;
                while (!((PeerServerImpl) servers.get(0)).isWorking()) {
                    Thread.onSpinWait();
                }
                ((PeerServerImpl) servers.remove(leaderIndex)).shutdown();
                System.out.println("Server " + (leaderIndex + 1) + " shutdown");
            }
        }

        // get response
        for (int i = 0; i < clients.size(); i++) {
            Client client = clients.get(i);
            HttpResponse<String> response;
            long startTime = System.currentTimeMillis();
            while ((response = client.getResponse()) == null &&
                    System.currentTimeMillis() - startTime < 100000) {
                Thread.onSpinWait();
            }

            // check results
            assertTrue(response != null, "Client response timeout");
            printMessage(response);
            assertTrue(response.body().startsWith("Work done for time: "), "Bad server response");
        }
    }

    // TODO make a test for worker failure

    // ---------------------- SERVER LOGIC ----------------------

    // ! Retrieve the leader InetSocketAddress

    public InetSocketAddress getGatewayAddress(List<PeerServer> servers) {
        return new InetSocketAddress("localhost", 8888);
    }

    // ! Print the leader and follower layout for a list of servers

    public void printServersLeader(List<PeerServer> servers) {
        for (PeerServer peerServer : servers) {
            long timer = System.currentTimeMillis();
            Vote leader;
            while ((leader = peerServer.getCurrentLeader()) == null) {
                if (System.currentTimeMillis() - timer > 5000) {
                    throw new IllegalStateException("Server "
                            + peerServer.getServerId()
                            + " is "
                            + peerServer.getPeerState());
                }
            }
            System.out.println("Server [" +
                    peerServer.getServerId() +
                    "] has leader [" +
                    leader.getProposedLeaderID() +
                    "] & current state: " +
                    peerServer.getPeerState().name());
        }
    }

    // ! Construct the system with a given amount of servers

    private static final int GATEWAYPORT = 8888;

    public void constructSystem(int peerServerCount) {
        try {
            List<PeerServer> servers = new ArrayList<>(peerServerCount);
            Map<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(peerServerCount + 1);
            for (long i = 1; i <= peerServerCount; i++) {
                peerIDtoAddress.put(i, new InetSocketAddress("localhost", getNextPort()));
            }
            int gatewayPort = getNextPort();
            long gatewayID = peerServerCount + 1;
            GatewayServer gatewayServer = new GatewayServer(
                    GATEWAYPORT,
                    gatewayPort,
                    0,
                    gatewayID,
                    new ConcurrentHashMap<>(peerIDtoAddress),
                    1);
            registerGateway(gatewayServer);
            gatewayServer.start();
            peerIDtoAddress.put(gatewayID, new InetSocketAddress("localhost", gatewayPort));
            for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
                if (entry.getKey() != gatewayID) {
                    ConcurrentHashMap<Long, InetSocketAddress> map = new ConcurrentHashMap<Long, InetSocketAddress>(
                            peerIDtoAddress);
                    map.remove(entry.getKey());
                    PeerServerImpl server = new PeerServerImpl(
                            entry.getValue().getPort(),
                            0,
                            entry.getKey(),
                            map,
                            gatewayID,
                            1);
                    servers.add(server);
                    server.start();
                }
            }
            registerServers(servers);
        } catch (IOException e) {
            e.printStackTrace();
            assert false;
        }
    }

    // ! Get the PeerServers

    public List<PeerServer> getServers() {
        if (this.registeredServers == null) {
            throw new IllegalStateException();
        }
        return new ArrayList<>(this.registeredServers);
    }

    // ! Get the GatewayServer

    public GatewayServer getGatewayServer() {
        if (this.registeredGatewayServer == null) {
            throw new IllegalStateException();
        }
        return this.registeredGatewayServer;
    }

    // --------------------- CLIENT METHODS ---------------------

    // ! Assemble clients

    public List<Client> getClients(int amount, InetSocketAddress gatewayAddress, String code) {
        List<Client> clients = new ArrayList<Client>(amount);
        for (int i = 0; i < amount; i++) {
            clients.add(new Client(i, gatewayAddress, code));
        }
        registerClients(clients);
        return clients;
    }

    // --------------------- HELPER METHODS ---------------------

    // ! Print all parts of an HttpResponse

    public void printMessage(HttpResponse<String> message) {
        System.out.println("<--- HTTP Response");
        System.out.println("Code: " + message.statusCode());
        System.out.println("Cache: " + message.headers().firstValue("Cached-Response").orElse("N/A"));
        System.out.println("Content:\n" + message.body());
        System.out.println("--->");
    }

    // -------------------- METHOD ISOLATION --------------------

    // ! logic for ensuring shutdown

    private List<Client> registeredClients;
    private List<PeerServer> registeredServers;
    private GatewayServer registeredGatewayServer;

    public void registerClients(List<Client> registeredClients) {
        this.registeredClients = registeredClients;
    }

    public void registerServers(List<PeerServer> registeredServers) {
        this.registeredServers = registeredServers;
    }

    public void registerGateway(GatewayServer gatewayServer) {
        this.registeredGatewayServer = gatewayServer;
    }

    @AfterEach
    public void shutdown() {
        if (this.registeredClients != null) {
            this.registeredClients = null;
        }
        if (this.registeredServers != null) {
            for (PeerServer server : this.registeredServers) {
                if (server != null) {
                    server.shutdown();
                }
            }
            this.registeredServers = null;
        }
        if (this.registeredGatewayServer != null) {
            this.registeredGatewayServer.shutdown();
            this.registeredServers = null;
        }

        cleanPorts();
    }

    // ! logic for separating test prints

    private static int testCount = 1;

    @BeforeEach
    public void printStartLine() {
        System.out.println("--------- test " + testCount++ + " ---------");
    }

    // ! Logic for making sure that test ports are open and ready

    private List<Integer> usedPortsList;

    private int getNextPort() {
        if (this.usedPortsList == null) {
            this.usedPortsList = new ArrayList<>();
        } else if (this.usedPortsList.size() >= 200) {
            throw new UnsupportedOperationException("Can't allocate more ports");
        }
        if (8000 + (this.usedPortsList.size() * 4) == GATEWAYPORT) {
            this.usedPortsList.add(8004 + (4 * this.usedPortsList.size()));
        } else {
            this.usedPortsList.add(8000 + (4 * this.usedPortsList.size()));
        }
        return this.usedPortsList.get(this.usedPortsList.size() - 1);
    }

    private void cleanPorts() {
        for (int portNum : this.usedPortsList) {
            InetSocketAddress udp = new InetSocketAddress("localhost", portNum);
            InetSocketAddress tcp = new InetSocketAddress("localhost", portNum + 2);
            boolean portOpen;
            long start = System.currentTimeMillis();
            while (!(portOpen = openUDP(udp))) {
                if (System.currentTimeMillis() - start > 5000) {
                    break;
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                }
            }
            if (!portOpen) {
                System.out.println("Port " + udp.getPort() + "; " + this.portException.getMessage());
                throw new IllegalStateException("Ports not clean - UDP:" + udp.getPort());
            }
            start = System.currentTimeMillis();
            while (!(portOpen = openTCP(tcp))) {
                if (System.currentTimeMillis() - start > 5000) {
                    break;
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                }
            }
            if (!portOpen) {
                System.out.println("Port " + tcp.getPort() + "; " + this.portException.getMessage());
                throw new IllegalStateException("Ports not clean - TCP:" + tcp.getPort());
            }
        }
        this.usedPortsList.clear();
    }

    private IOException portException;

    private boolean openUDP(InetSocketAddress port) {
        DatagramSocket udp;
        try {
            udp = new DatagramSocket(port);
            udp.close();
        } catch (IOException e) {
            this.portException = e;
            return false;
        }
        return true;
    }

    private boolean openTCP(InetSocketAddress port) {
        ServerSocket tcp;
        try {
            tcp = new ServerSocket();
            tcp.setReuseAddress(true);
            tcp.bind(port);
            tcp.close();
        } catch (IOException e) {
            this.portException = e;
            return false;
        }
        return true;
    }

    // ! Logic for cleaning up old log files

    @AfterAll
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
            while (dirs.size() > 3) {
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

    // ------------------------- CLIENT -------------------------

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

        private void setBadContext() {
            try {
                this.uri = new URI("http", null, this.uri.getHost(), this.uri.getPort(),
                        "/badContext", null, null);
            } catch (URISyntaxException e) {
                throw new IllegalStateException(e);
            }
        }

        private void setBadMethod() {
            this.request = HttpRequest.newBuilder(this.uri).header(HEADER_NAME, HEADER_VALUE)
                    .PUT(BodyPublishers.ofString(this.code)).build();
        }

        private void setBadHeaders() {
            this.request = HttpRequest.newBuilder(this.uri).header(HEADER_NAME, "foo")
                    .POST(BodyPublishers.ofString(this.code)).build();
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

    // -------------------------- CODE --------------------------

    private final static String HELLO = "public class Hello { public String run() { return \"Hello\"; } }";
    private final static String VAR = "public class Variable { public String run() { return \"var\"; } }";
    private final static String NULL = "public class Null { public String run(){ return null; } }";
    private final static String ERR = "public class Error { public String run(){ Error no \";\" } }";
    private final static String THROW = "public class Throw { public String run(){ throw new IllegalArgumentException(); } }";
    private final static String WORK10 = "public class Work { public String run() { try { Thread.sleep(10); }"
            + " catch (InterruptedException e) { return \"Work Interrupted\"; }"
            + " return \"Work done for time: \" + 10; } }";
    private final static String RUN = "public class Print {public String run() {System.out.println(\"Running on thread:"
            + " \" + Thread.currentThread().threadId());return \"printed running\";}}";
}
