import ch.heig.mac.Indices;
import ch.heig.mac.Main;
import ch.heig.mac.Requests;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryOutputFormatTest {

    private Cluster cluster;
    private Requests requests;
    private Indices indices;

    @BeforeEach
    public void setUp() {
        cluster = Main.openConnection();
        requests = new Requests(cluster);
        indices = new Indices(cluster);
    }

    @AfterEach
    void tearDown() {
        cluster.disconnect();
    }

    @Test
    public void testGetCollectionNamesQuery() {
        assertThat(requests.getCollectionNames())
                .hasSameElementsAs(List.of("comments", "movies", "theaters", "users"));
    }

    @Test
    public void testInconsistentRatingQuery() {
        indices.createRequiredIndicesOf(1);

        JsonObject row = requests.inconsistentRating().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("imdb_id", "tomato_rating", "imdb_rating"));
    }

    @Test
    public void testTopReviewersQuery() {
        indices.createRequiredIndicesOf(2);

        JsonObject row = requests.topReviewers().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("name", "cnt"));
    }

    @Test
    public void testBestMoviesOfActorQuery() {
        indices.createRequiredIndicesOf(4);

        JsonObject row = requests.bestMoviesOfActor("Al Pacino").get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("id", "rating", "cast"));
    }

    @Test
    public void testPlentifulDirectorsQuery() {
        indices.createRequiredIndicesOf(5);

        JsonObject row = requests.plentifulDirectors().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("director_name", "count_film"));
    }

    @Test
    public void testConfusingMoviesQuery() {
        indices.createRequiredIndicesOf(6);

        JsonObject row = requests.confusingMovies().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("movie_id", "title"));
    }

    @Test
    public void testCommentsOfDirector1Query() {
        indices.createRequiredIndicesOf(7);

        JsonObject row = requests.commentsOfDirector1("Woody Allen").get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("movie_id", "text"));
    }

    @Test
    public void testCommentsOfDirector2Query() {
        indices.createRequiredIndicesOf(7);

        JsonObject row = requests.commentsOfDirector2("Woody Allen").get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("movie_id", "text"));
    }

    @Test
    public void testNightMoviesQuery() {
        indices.createRequiredIndicesOf(9);

        // ensure at least one movie is only projected late
        requests.removeEarlyProjection("573a13edf29313caabdd49ad");

        JsonObject row = requests.nightMovies().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("movie_id", "title"));
    }
}
