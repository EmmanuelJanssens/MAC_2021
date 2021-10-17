package ch.heig.mac;

import java.util.LinkedList;
import java.util.List;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;


public class Requests {
    private final Cluster cluster;

    public Requests(Cluster cluster) {
        this.cluster = cluster;
    }

    public List<String> getCollectionNames() {
        QueryResult result = cluster.query(
                "SELECT RAW r.name\n" +
                        "FROM system:keyspaces r\n" +
                        "WHERE r.`bucket` = \"mflix-sample\";"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> inconsistentRating() {
        QueryResult result = cluster.query(
                "SELECT imdb.id AS imdb_id, tomatoes.viewer.rating AS tomato_rating, imdb.rating AS imdb_rating\n" +
                        "FROM `mflix-sample`.`_default`.`movies`\n" +
                        "WHERE `tomatoes`.`viewer`.`rating` != 0\n" +
                        "AND ABS(`imdb`.`rating` - `tomatoes`.`viewer`.`rating`) > 7;"
        );
        return result.rowsAsObject();
    }

    /**
     * Pour retourner le nom des 10 personnes ayant fait le plus de
     * commentaires ainsi que le nombre de leurs commentaires(cnt)
     * @return
     */
    public List<JsonObject> topReviewers() {
        QueryResult result = cluster.query(
        "SELECT name, COUNT(name) as cnt " +
                "FROM `mflix-sample`.`_default`.`comments` " +
                "GROUP BY name " +
                "ORDER BY COUNT(name) DESC " +
                "LIMIT 10;"
        );

        return result.rowsAsObject();
    }

    public List<String> greatReviewers() {
        QueryResult result = cluster.query(
                "SELECT name\n" +
                        "FROM `mflix-sample`.`_default`.`comments`\n" +
                        "GROUP BY name\n" +
                        "HAVING COUNT(*) > 300;"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        String query =
                "SELECT imdb.id, imdb.rating, `cast` " +
                        "FROM `mflix-sample`._default.movies " +
                        "WHERE $actor WITHIN `cast`  AND  imdb.rating > 9 "+
                        "ORDER BY imdb.rating DESC ";

        QueryResult result = cluster.query(query,
                (QueryOptions.queryOptions()).parameters(
                        JsonObject.create().put("actor",actor)
                ));
        return result.rowsAsObject();
    }

    public List<JsonObject> plentifulDirectors() {
        String query =
                "SELECT  directors as director_name, count_film\n" +
                "FROM `mflix-sample`._default.movies as movies\n" +
                "UNNEST movies.directors\n" +
                "GROUP BY directors\n" +
                "LETTING count_film = COUNT(movies.title)\n" +
                "HAVING count_film > 30";

        QueryResult result = cluster.query(query);

        return result.rowsAsObject();
    }

    public List<JsonObject> confusingMovies() {
        QueryResult result = cluster.query(
                "SELECT _id as movie_id,title " +
                        "FROM `mflix-sample`._default.movies " +
                        "WHERE ARRAY_COUNT(directors) > 20"
        );

        return result.rowsAsObject();
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        String query =
                "SELECT c.text,c.movie_id\n" +
                        "FROM `mflix-sample`._default.comments c\n" +
                        "JOIN `mflix-sample`._default.movies m\n" +
                        "ON c.movie_id = m._id\n" +
                        "WHERE ARRAY_CONTAINS(m.directors,$director)\n";
        QueryResult result = cluster.query(query,QueryOptions.queryOptions()
        .parameters(
                JsonObject.create().put("director",director)
        ));

        return result.rowsAsObject();
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        String query =
                "SELECT c.text, c.movie_id\n" +
                "FROM `mflix-sample`._default.comments c\n" +
                "WHERE c.movie_id IN (SELECT RAW _id  FROM `mflix-sample`._default.movies WHERE $director IN directors)";

        QueryResult result = cluster.query(query,
                QueryOptions.queryOptions()
        .parameters(
                JsonObject.create().put("director",director)
        ));

        return result.rowsAsObject();
    }

    // Returns true if the update was successful.
    public Boolean removeEarlyProjection(String movieId) {
        String query =
                "UPDATE `mflix-sample`._default.theaters\n" +
                "SET schedule = ARRAY v FOR v IN schedule WHEN v.hourBegin > \"18:00:00\" END  \n" +
                "WHERE ANY v IN schedule SATISFIES v.hourBegin<=\"18:00:00\" AND v.movieId = $movieId END";
        QueryResult result = cluster.query(query,
                (QueryOptions.queryOptions()).parameters(
                        JsonObject.create().put("movieId",movieId)
                ) );

        return result.metaData().status() == QueryStatus.SUCCESS;
    }

    public List<JsonObject> nightMovies() {

        //Retourner les films qui sont projeté uniquement après 18h
        String query =
                "SELECT  movies._id as movie_id,movies.title\n" +
                        "FROM `mflix-sample`._default.movies as movies\n" +
                        "JOIN \n" +
                        "(SELECT  sched.movieId\n" +
                        "FROM `mflix-sample`._default.theaters \n" +
                        "USE INDEX(theater_movie_id)\n" +
                        "UNNEST theaters.schedule AS sched \n" +
                        "WHERE sched.hourBegin > \"18:00:00\") AS result\n" +
                        "ON result.movieId = movies._id";
        QueryResult result = cluster.query(query);
        return result.rowsAsObject();
    }


}
