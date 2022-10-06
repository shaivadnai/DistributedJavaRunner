package edu.yu.cs.com3800;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.yu.cs.com3800.ClientImpl.Response;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

public class HttpTests {
    static List<ZooKeeperPeerServerImpl> servers;
    static ClientImpl client;
    Response response;
    HttpClient testClient;

    String happyShortSingleLine = "public class HelloWorld {public HelloWorld(){}public String run(){return \"happyShortSingleLine\";}}";
    String happyShortMultiLine = "public class HelloWorld { \n public HelloWorld(){\n}\npublic String run(){\nreturn \"happyShortMultiLine\";\n}\n}";
    String badShortSingleLine = "public class HelloWorld {public HelloWorld(){}public String run(){return \"happyShortSingleLine\"}}";
    String badShortMultiLine = "public class HelloWorld { \n public HelloWorld(){\n}\npublic String run(){\nreturn \"happyShortMultiLine\"\n}\n}";
    String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return Thread.currentThread().getName();\n    }\n}\n";
    String incrementingClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"1\";\n    }\n}\n";
    String delayedClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n   try{Thread.sleep(90000);}catch(Exception e){}    \n return Thread.currentThread().getName();\n    }\n}\n";

    static int workerCount = 8;

    @Test
    public void happyShortSingleLine() throws Exception {
        client.sendCompileAndRunRequest(happyShortSingleLine);

        response = client.getResponse();

        assertEquals(200, response.getCode());

        assertEquals("happyShortSingleLine", response.getBody());
        judahPrintResponse("happyShortSingleLine", response.getBody());
    }

    @Test
    public void happyShortMultiLine() throws Exception {
        client.sendCompileAndRunRequest(happyShortMultiLine);

        response = client.getResponse();

        assertEquals(200, response.getCode());

        assertEquals("happyShortMultiLine", response.getBody());
        judahPrintResponse("happyShortMultiLine", response.getBody());
    }

    @Test
    public void badShortSingleLine() throws Exception {
        client.sendCompileAndRunRequest(badShortSingleLine);

        response = client.getResponse();

        assertEquals(400, response.getCode());

        String expectedString = getResponse(badShortSingleLine);
        String[] expectedArray = Arrays.copyOf(expectedString.split("\n"), 8);

        String[] actualArray = Arrays.copyOf(response.getBody().split("\n"), 8);
        assertArrayEquals(expectedArray, actualArray);
        judahPrintResponse(arrayToString(expectedArray), arrayToString(actualArray));
    }

    @Test
    public void badShortMultiLine() throws Exception {
        client.sendCompileAndRunRequest(badShortMultiLine);

        response = client.getResponse();

        assertEquals(400, response.getCode());

        String expectedString = getResponse(badShortMultiLine);
        String[] expectedArray = Arrays.copyOf(expectedString.split("\n"), 8);

        String[] actualArray = Arrays.copyOf(response.getBody().split("\n"), 8);
        assertArrayEquals(expectedArray, actualArray);
        judahPrintResponse(arrayToString(expectedArray), arrayToString(actualArray));
    }

    @Test
    public void multipleRequests() throws Exception {
        client.sendCompileAndRunRequest(happyShortSingleLine);

        response = client.getResponse();

        assertEquals(200, response.getCode());
        judahPrintResponse(200, response.getCode());

        assertEquals("happyShortSingleLine", response.getBody());
        judahPrintResponse("happyShortSingleLine", response.getBody());

        client.sendCompileAndRunRequest(happyShortMultiLine);

        response = client.getResponse();

        assertEquals(200, response.getCode());
        judahPrintResponse(200, response.getCode());

        assertEquals("happyShortMultiLine", response.getBody());
        judahPrintResponse("happyShortMultiLine", response.getBody());

        client.sendCompileAndRunRequest(badShortSingleLine);

        response = client.getResponse();

        assertEquals(400, response.getCode());
        judahPrintResponse(400, response.getCode());

        String expectedString = getResponse(badShortSingleLine);
        String[] expectedArray = Arrays.copyOf(expectedString.split("\n"), 8);

        String[] actualArray = Arrays.copyOf(response.getBody().split("\n"), 8);
        assertArrayEquals(expectedArray, actualArray);
        judahPrintResponse(arrayToString(expectedArray), arrayToString(actualArray));

        client.sendCompileAndRunRequest(badShortMultiLine);

        response = client.getResponse();

        assertEquals(400, response.getCode());
        judahPrintResponse(400, response.getCode());

        expectedString = getResponse(badShortMultiLine);
        expectedArray = Arrays.copyOf(expectedString.split("\n"), 8);

        actualArray = Arrays.copyOf(response.getBody().split("\n"), 8);
        assertArrayEquals(expectedArray, actualArray);
        judahPrintResponse(arrayToString(expectedArray), arrayToString(actualArray));
    }

    @Test
    public void longSrcCode() throws Exception {
        client.sendCompileAndRunRequest(generateLongProgram(1 << 15));

        response = client.getResponse();
        String expectedString = getResponse(generateLongProgram(1 << 15));

        assertEquals(200, response.getCode());

        assertEquals(expectedString, response.getBody());
        judahPrintResponse(expectedString, response.getBody());
    }

    @Test
    public void wrongContentTypeHeader() throws InterruptedException, ExecutionException, IOException {
        client.sendCompileAndRunBadContentType(happyShortSingleLine);
        response = client.getResponse();
        assertEquals(400, response.getCode());
        judahPrintResponse(400, response.getCode());

        assertEquals("", response.getBody());
        judahPrintResponse("", response.getBody());
    }

    @Test
    public void illegalCharachters() throws Exception {
        client.sendCompileAndRunRequest(generateLongProgram(1 << 15));

        response = client.getResponse();

        assertEquals(200, response.getCode());
        judahPrintResponse(200, response.getCode());
        assertEquals("happyShortMultiLine", response.getBody());
        judahPrintResponse("happyShortMultiLine", response.getBody());
    }

    @Test
    public void multipleWorkersAreFulfillingRequests() throws IOException {
        Set<String> followersThatResponded = new HashSet<>();
        List<ClientImpl> clients = new LinkedList<>();
        int messageNumber = 15;
        int count = -1;
        for (int i = 0; i < messageNumber; i++) {
            ClientImpl client = new ClientImpl("localhost", 8888);
            if (count++ % 10 == 0) {
                System.out.println(count + " messages sent");
            }
            client.sendCompileAndRunRequest(validClass);
            clients.add(client);
        }
        count = -1;
        for (ClientImpl c : clients) {
            if (count++ % 5 == 0) {
                System.out.println(count + " messages received");
            }
            response = c.getResponse();
            assertEquals(200, response.getCode());
            assertTrue(response.getBody().contains("Follower"));
            followersThatResponded.add(response.getBody());
        }
        // assertTrue(workerCount == followersThatResponded.size());
    }

    @Test
    public void correctResponseToCorrectClient() throws IOException {
        Set<String> followersThatResponded = new HashSet<>();
        List<ClientImpl> clients = new LinkedList<>();
        int messageNumber = 15;
        int count = -1;
        for (int i = 0; i < messageNumber; i++) {
            ClientImpl client = new ClientImpl("localhost", 8888);
            client.sendCompileAndRunRequest(incrementingClass.replace("\"1\"", "\"" + i + "\""));
            clients.add(client);
            if (count++ % 5 == 0) {
                System.out.println(count + " messages sent");
            }
        }
        int i = 0;
        count = 0;
        for (ClientImpl c : clients) {
            if (count++ % 5 == 0) {
                System.out.println(count + " messages received");
            }
            response = c.getResponse();
            assertEquals(200, response.getCode());
            assertTrue(response.getBody().contains(String.valueOf(i++)));
            followersThatResponded.add(response.getBody());
        }
    }

    @Test
    public void killedFollower() throws IOException {
        Set<String> followersThatResponded = new HashSet<>();
        List<ClientImpl> clients = new LinkedList<>();
        int messageNumber = 15;
        int count = -1;
        for (int i = 0; i < messageNumber; i++) {
            ClientImpl client = new ClientImpl("localhost", 8888);
            if (count++ % 10 == 0) {
                System.out.println(count + " messages sent");
            }
            client.sendCompileAndRunRequest(validClass);
            clients.add(client);
        }
        Cluster.killFollower(servers);
        count = -1;
        for (ClientImpl c : clients) {
            if (count++ % 5 == 0) {
                System.out.println(count + " messages received");
            }
            response = c.getResponse();
            assertEquals(200, response.getCode());
            assertTrue(response.getBody().contains("Follower"));
            followersThatResponded.add(response.getBody());
        }
    }

    /*
     * @Test
     * public void killedLeader() throws IOException {
     * Set<String> followersThatResponded = new HashSet<>();
     * List<ClientImpl> clients = new LinkedList<>();
     * int messageNumber = 15;
     * int count = -1;
     * for (int i = 0; i < messageNumber; i++) {
     * ClientImpl client = new ClientImpl("localhost", 8888);
     * if (count++ % 10 == 0) {
     * System.out.println(count + " messages sent");
     * }
     * client.sendCompileAndRunRequest(validClass);
     * clients.add(client);
     * }
     * Cluster.killLeader(servers);
     * count = -1;
     * for (ClientImpl c : clients) {
     * if (count++ % 5 == 0) {
     * System.out.println(count + " messages received");
     * }
     * response = c.getResponse();
     * 
     * assertTrue(response.getBody().contains("Follower"));
     * followersThatResponded.add(response.getBody());
     * }
     * }
     */

    /**
     * Requires that one checks the logs to see the old work was retrieved
     * correctly.
     * Duration of delayedClass must be adjusted based on the cluster size and
     * machine
     * 
     * 
     * TAKES FOREVER TO RUN.
     * 
     * @throws IOException
     */
    @Test
    public void killedLeaderCheckGatherOldWork() throws IOException {
        Set<String> followersThatResponded = new HashSet<>();
        List<ClientImpl> clients = new LinkedList<>();
        int messageNumber = 5;
        int count = -1;
        for (int i = 0; i < messageNumber; i++) {
            ClientImpl client = new ClientImpl("localhost", 8888);
            if (count++ % 10 == 0) {
                System.out.println(count + " messages sent");
            }
            client.sendCompileAndRunRequest(delayedClass);
            clients.add(client);
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Cluster.killLeader(servers);
        count = -1;
        for (ClientImpl c : clients) {
            if (count++ % 5 == 0) {
                System.out.println(count + " messages received");
            }
            response = c.getResponse();

            assertTrue(response.getBody().contains("Follower"));
            followersThatResponded.add(response.getBody());
        }
        System.out.println("Check Logs to Confirm Got Old Work Successfully");
    }

    private String arrayToString(String[] array) {
        StringBuilder sb = new StringBuilder();
        for (String i : array) {
            sb.append(i);
            sb.append("\n");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    private void judahPrintResponse(String expected, String actual) {
        System.out.println("Expected response:");
        System.out.println(expected);
        System.out.println("Actual response:");
        System.out.println(actual);
    }

    private void judahPrintResponse(int expected, int actual) {
        System.out.println("Expected response:");
        System.out.println(expected);
        System.out.println("Actual response:");
        System.out.println(actual);
    }

    private String generateLongProgram(int size) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "public class HelloWorld { \n public HelloWorld(){\n}\npublic String run(){\nreturn \"happyShortMultiLine\";\n}\n}\n");
        while (size > 0) {
            sb.append("//1234567890");
            sb.append("\n");
            size -= 12;
        }
        return sb.toString();
    }

    private String getResponse(String src) throws Exception {
        try {
            JavaRunner runner = new JavaRunner();
            return runner.compileAndRun(new ByteArrayInputStream(src.getBytes()));
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            sb.append(e.getMessage());
            sb.append("\n");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            sb.append(sw.toString());
            return sb.toString();
        }
    }

    @BeforeClass
    public static void init() throws Exception {
        client = new ClientImpl("localhost", 8888);
        while (Thread.activeCount() > 20) {
            System.out.println(Thread.activeCount() + " running, waiting until 20");
            Thread.sleep(500);
        }
        servers = Cluster.initCluster(workerCount + 1);
    }

    @AfterClass
    public static void clean() throws Exception {
        Cluster.shutdownServers(servers);
    }
}
