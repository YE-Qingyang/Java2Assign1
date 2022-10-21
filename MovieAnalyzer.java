import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieAnalyzer {

  public static class Movie {
    private String seriesTitle;
    private int releasedYear;
    private String certificate;
    private int runtime;
    private List<String> genre;
    private float rating;
    private String overview;
    private int score;
    private String director;
    private List<String> star;
    private long votes;
    private long gross;

    public Movie(String seriesTitle, int releasedYear, String certificate, int runtime,
                 String genre, float rating, String overview, int score, String director,
                 String[] star, long votes, long gross) {
      this.seriesTitle = seriesTitle;
      this.releasedYear = releasedYear;
      this.certificate = certificate;
      this.runtime = runtime;
      this.genre = Arrays.stream(genre.replaceAll("\"", "").split(", ")).toList();
      this.rating = rating;
      this.overview = overview;
      this.score = score;
      this.director = director;
      this.star = Arrays.stream(star).toList();
      this.votes = votes;
      this.gross = gross;
    }

    public String getSeriesTitle() {
      return seriesTitle;
    }

    public int getReleasedYear() {
      return releasedYear;
    }

    public String getCertificate() {
      return certificate;
    }

    public int getRuntime() {
      return runtime;
    }

    public List<String> getGenre() {
      return genre;
    }

    public float getRating() {
      return rating;
    }

    public String getOverview() {
      return overview;
    }

    public int getScore() {
      return score;
    }

    public String getDirector() {
      return director;
    }

    public List<String> getStar() {
      return star;
    }

    public long getVotes() {
      return votes;
    }

    public long getGross() {
      return gross;
    }
  }

  Stream<Movie> movies;
  List<Movie> moviesList;

  public MovieAnalyzer(String datasetPath) throws IOException {
    Stream<String> stream = Files.lines(Paths.get(datasetPath), StandardCharsets.UTF_8).skip(1);

    movies = stream.map(l -> (l + " ").split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
            .map(a -> new Movie(a[1].replaceAll("\"", "").strip(),
                    Integer.parseInt(a[2]),
                    a[3],
                    Integer.parseInt(a[4].replace(" min", "").strip()),
                    a[5].replaceAll("\"", "").strip(),
                    Float.parseFloat(a[6]),
                    a[7].replaceAll("^\"*|\"*$", "").strip(),
                    Integer.parseInt(a[8].equals("") ? "0" : a[8]),
                    a[9],
                    new String[]{a[10], a[11], a[12], a[13]},
                    Long.parseLong(a[14]),
                    Long.parseLong(a[15].equals(" ") ? "0" : a[15].replaceAll(",", "")
                            .replaceAll("\"", "").strip())));

    moviesList = new ArrayList<>(movies.toList());
  }

  //1. Movie count by year
  public Map<Integer, Integer> getMovieCountByYear() {
    Stream<Movie> movies = this.moviesList.stream();

    Map<Integer, Long> movieCountByYear = movies.collect(Collectors
            .groupingBy(Movie::getReleasedYear, Collectors.counting()));

    // sorting by descending order of year
    List<Map.Entry<Integer, Long>> list = new LinkedList<>(movieCountByYear.entrySet());
    list.sort((o1, o2) -> o2.getKey().compareTo(o1.getKey()));

    Map<Integer, Integer> movieCountByYearDesc = new LinkedHashMap<>();

    list.forEach(e -> movieCountByYearDesc.put(e.getKey(), e.getValue().intValue()));

    return movieCountByYearDesc;
  }

  //2. Movie count by genre
  public Map<String, Integer> getMovieCountByGenre() {
    Map<String, Integer> movieCountByGenre = new LinkedHashMap<>();
    Stream<Movie> movies = this.moviesList.stream();

    movies.forEach(m -> m.getGenre().forEach(g -> {
      if (movieCountByGenre.containsKey(g)) {
        movieCountByGenre.replace(g, movieCountByGenre.get(g) + 1);
      } else {
        movieCountByGenre.put(g, 1);
      }
    }));

    // sorting by descending order of count
    // then sorting by the alphabetical order of genre
    List<Map.Entry<String, Integer>> list = new LinkedList<>(movieCountByGenre.entrySet());
    list.sort((o1, o2) -> {
      if (!Objects.equals(o1.getValue(), o2.getValue())) {
        return o2.getValue().compareTo(o1.getValue());
      } else {
        return o1.getKey().compareTo(o2.getKey());
      }
    });

    Map<String, Integer> movieCountByGenreDesc = new LinkedHashMap<>();

    list.forEach(e -> movieCountByGenreDesc.put(e.getKey(), e.getValue()));

    return movieCountByGenreDesc;
  }

  //3. Movie count by co-stars
  public Map<List<String>, Integer> getCoStarCount() {
    Map<List<String>, Integer> coStarCount = new LinkedHashMap<>();
    Stream<Movie> movies = this.moviesList.stream();

    movies.forEach(m -> {
      for (int i = 0; i < m.getStar().size(); i++) {
        for (int j = i + 1; j < m.getStar().size(); j++) {
          List<String> l = new LinkedList<>();
          if (m.getStar().get(i).compareTo(m.getStar().get(j)) <= 0) {
            l.add(m.getStar().get(i));
            l.add(m.getStar().get(j));
          } else {
            l.add(m.getStar().get(j));
            l.add(m.getStar().get(i));
          }
          if (coStarCount.containsKey(l)) {
            coStarCount.replace(l, coStarCount.get(l) + 1);
          } else {
            coStarCount.put(l, 1);
          }
        }
      }
    });

    return coStarCount;
  }

  //4. Top Movies
  public List<String> getTopMovies(int top_k, String by) {

    List<Movie> topMovies = this.moviesList;

    if (by.equals("runtime")) {
      topMovies.sort((o1, o2) -> {
        if (o1.getRuntime() != o2.getRuntime()) {
          return o2.getRuntime() - o1.getRuntime();
        } else {
          return o1.getSeriesTitle().compareTo(o2.getSeriesTitle());
        }
      });
    }

    if (by.equals("overview")) {
      topMovies.sort((o1, o2) -> {
        if (o1.getOverview().length() != o2.getOverview().length()) {
          return o2.getOverview().length() - o1.getOverview().length();
        }  else {
          return o1.getSeriesTitle().compareTo(o2.getSeriesTitle());
        }
      });
    }

    List<String> topMoviesTitle = new LinkedList<>();

    topMovies.subList(0, top_k).forEach(m -> topMoviesTitle.add(m.getSeriesTitle()));

    return topMoviesTitle;
  }

  //5. Top stars
  public List<String> getTopStars(int top_k, String by) {

    List<Movie> list = this.moviesList;

    Map<String, Double[]> avg = new LinkedHashMap<>();

    if (by.equals("rating")) {
      list.forEach(m -> {
        if (m.getRating() == 0) {
          return;
        }
        m.getStar().forEach(s -> {
          if (avg.containsKey(s)) {
            avg.replace(s, new Double[]{(avg.get(s)[0] + m.getRating()), (avg.get(s)[1] + 1)});
          } else {
            avg.put(s, new Double[]{(double) m.getRating(), 1.0});
          }
        });
      });
    }

    if (by.equals("gross")) {
      list.forEach(m -> {
        if (m.getGross() == 0) {
          return;
        }
        m.getStar().forEach(s -> {
          if (avg.containsKey(s)) {
            avg.replace(s, new Double[]{(avg.get(s)[0] + m.getGross()), (avg.get(s)[1] + 1)});
          } else {
            avg.put(s, new Double[]{(double) m.getGross(), 1.0});
          }
        });
      });
    }

    Map<String, Double> top = new LinkedHashMap<>();
    avg.forEach((k, v) -> top.put(k, v[0] / v[1]));

    List<Map.Entry<String, Double>> topStarsMap = new LinkedList<>(top.entrySet());
    topStarsMap.sort((o1, o2) -> {
      if (!Objects.equals(o1.getValue(), o2.getValue())) {
        return o2.getValue().compareTo(o1.getValue());
      } else {
        return o1.getKey().compareTo(o2.getKey());
      }
    });

    List<String> topStars = new LinkedList<>();
    topStarsMap.subList(0, top_k).forEach(s -> topStars.add(s.getKey()));

    return topStars;
  }

  //6. Search Movies
  public List<String> searchMovies(String genre, float min_rating, int max_runtime) {

    List<Movie> movies = this.moviesList;

    movies = movies.stream().filter(m ->
      m.getGenre().contains(genre)).filter(m ->
      m.getRating() >= min_rating).filter(m ->
      m.getRuntime() <= max_runtime).toList();

    List<String> movieTitle = new ArrayList<>();
    movies.forEach(m -> movieTitle.add(m.getSeriesTitle()));

    movieTitle.sort((Comparator.naturalOrder()));

    return movieTitle;
  }
}