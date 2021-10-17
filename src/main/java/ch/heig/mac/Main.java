package ch.heig.mac;

import com.couchbase.client.java.Cluster;

public class Main {

    // TODO: Configure credentials to allow connection to your local Couchbase instance
    public static Cluster openConnection() {
        var connectionString = "127.0.0.1";
        var username = "Administrator";
        var password = "mac2021";

        Cluster cluster = Cluster.connect(
                connectionString,
                username,
                password
        );
        return cluster;
    }

    public static void main(String[] args) {
        var cluster = openConnection();

        var requests = new Requests(cluster);
        var indices = new Indices(cluster);

        indices.createRequiredIndices();


        //requests.getCollectionNames().forEach(System.out::println);


        // 2)
        //requests.topReviewers().forEach(System.out::println);

        //  4)
        //requests.bestMoviesOfActor("Al Pacino").forEach(System.out::println);

        //  5)
        //requests.plentifulDirectors().forEach(System.out::println);

        // 6)
        //requests.confusingMovies().forEach(System.out::println);

        //  7.1)
        //requests.commentsOfDirector1("Woody Allen").forEach(System.out::println);

        //  7.2)
        //requests.commentsOfDirector2("Woody Allen").forEach(System.out::println);



        // 8)
        //requests.removeEarlyProjection("");

        //9)
        requests.nightMovies().forEach(System.out::println);
    }
}
