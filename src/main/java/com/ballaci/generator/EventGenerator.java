package com.ballaci.generator;


import com.github.javafaker.Faker;

//import java.net.URI;
//import java.net.http.HttpClient;
//import java.net.URISyntaxException;
//import java.net.http.HttpRequest;
//import java.net.http.HttpResponse;
//import java.time.Duration;

public class EventGenerator {

//    static URI uri;
//    static Faker faker = new Faker();
//
//
//    private static final HttpClient httpClient = HttpClient.newBuilder()
//            .version(HttpClient.Version.HTTP_1_1)
//            .connectTimeout(Duration.ofSeconds(10))
//            .build();
//
//    public static void main(String[] args) throws InterruptedException, URISyntaxException {
//        int eventId = 0;
//        String[] types = {"type1", "type2"};
//        while (true) {
//            String type = faker.options().nextElement(types).toString();
//            postJSON(new URI("http://localhost:8080/event/" + eventId + "/" + type));
//            eventId++;
//            Thread.sleep(3000);
//        }
//
//    }
//
//    public static void postJSON(URI uri) {
//        try {
//            HttpRequest request = HttpRequest.newBuilder(uri)
//                    .header("Content-Type", "application/json")
//                    .POST(HttpRequest.BodyPublishers.ofString(""))
//                    .build();
//            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
//            System.out.println(response.statusCode());
//            System.out.println(response.body());
//        } catch (Exception e) {
//            System.out.println(e);
//        }
//    }
}
