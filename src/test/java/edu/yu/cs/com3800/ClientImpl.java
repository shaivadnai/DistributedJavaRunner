package edu.yu.cs.com3800;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ClientImpl {
    private HttpClient client;
    private CompletableFuture<HttpResponse<String>> future;
    private String hostName;
    private int hostPort;

    public ClientImpl(String hostName, int hostPort) throws MalformedURLException {
        client = HttpClient.newBuilder().build();
        this.hostName = hostName;
        this.hostPort = hostPort;
    }

    public void sendCompileAndRunRequest(String src) throws IOException {
        if (src == null) {
            throw new IllegalArgumentException();
        }
        Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://" + hostName + ":" + hostPort + "/compileandrun"))
                .headers("Content-Type", "text/x-java-source").POST(BodyPublishers.ofString(src));
        future = client.sendAsync(builder.build(), BodyHandlers.ofString());
    }

    public void sendCompileAndRunBadContentType(String src) {
        if (src == null) {
            throw new IllegalArgumentException();
        }
        Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://" + hostName + ":" + hostPort + "/compileandrun"))
                .headers("Content-Type", "text/x-python-source").POST(BodyPublishers.ofString(src));
        future = client.sendAsync(builder.build(), BodyHandlers.ofString());
    }

    public Response getResponse() throws IOException {
        if (future == null) {
            throw new IllegalStateException("There is no request for which to get a response");
        }
        try {
            HttpResponse<String> httpResponse = future.get();
            Response response = new Response(httpResponse.statusCode(), httpResponse.body());
            return response;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return new Response(501, "EMPTY - NORESPONSE");
    }

    class Response {
        private int code;
        private String body;

        public Response(int code, String body) {
            this.code = code;
            this.body = body;
        }

        public int getCode() {
            return this.code;
        }

        public String getBody() {
            return this.body;
        }
    }
}
