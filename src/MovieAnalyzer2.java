import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieAnalyzer2 {

    public ArrayList<Movie> movies;

    public class Movie {
        public String title;
        public int year;
        public String certificate;
        public int runtime;
        public List<String> genre;
        public double rating;
        public String overview;
        public double metascore;
        public String director;
        public List<String> stars;
        public double votes;
        public double gross;

        public Movie(String title, int year, String certificate, int runtime, List<String> genre, double rating, String overview, double metascore, String director, List<String> stars, double votes, double gross) {
            this.title = title;
            this.year = year;
            this.certificate = certificate;
            this.runtime = runtime;
            this.genre = genre;
            this.rating = rating;
            this.overview = overview;
            this.metascore = metascore;
            this.director = director;
            this.stars = stars;
            this.votes = votes;
            this.gross = gross;
        }

        public int getYear(){
            return year;
        }

        @Override
        public String toString() {
            return "Movie{" +
                    "title='" + title + '\'' +
                    ", year=" + year +
                    ", certificate='" + certificate + '\'' +
                    ", runtime=" + runtime +
                    ", genre='" + genre + '\'' +
                    ", rating=" + rating +
                    ", overview='" + overview + '\'' +
                    ", metascore=" + metascore +
                    ", director='" + director + '\'' +
                    ", stars=" + stars +
                    ", votes=" + votes +
                    ", gross=" + gross +
                    '}';
        }
    }


    public MovieAnalyzer2(String dataset_path) {
        movies = new ArrayList<>(500);
        try (BufferedReader br = Files.newBufferedReader(Paths.get(dataset_path))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                Movie movie = new Movie(columns[1], columns[2].isBlank() ? -1 : Integer.parseInt(columns[2]), columns[3], columns[4].isBlank() ? -1 : Integer.parseInt(columns[4].split(" ")[0]), Arrays.asList(columns[5].replace("\"","").replace(" ","").split(",")), columns[6].isBlank() ? -1f : Double.parseDouble(columns[6]), columns[7], columns[8].isBlank() ? -1f : Double.parseDouble(columns[8]), columns[9], Arrays.asList(columns[10], columns[11], columns[12], columns[13]), Double.parseDouble(columns[14]), columns.length < 16 ? -1f : Double.parseDouble(columns[15].substring(1, columns[15].length() - 1).replaceAll(",", "")));
//                System.out.println(movie);
                movies.add(movie);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public Map<Integer, Integer> getMovieCountByYear(){
        Stream<Movie> movieStream = movies.stream();
        return movieStream.collect(Collectors.groupingBy(Movie::getYear, Collectors.summingInt(e -> 1))).entrySet().stream().sorted(Map.Entry.comparingByKey(Comparator.reverseOrder())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    public Map<String, Integer> getMovieCountByGenre(){
        Stream<Movie> movieStream = movies.stream();
        return movieStream.flatMap(movie -> movie.genre.stream()).collect(Collectors.groupingBy(e -> e, Collectors.summingInt(e -> 1))).entrySet().stream().sorted(((item1, item2) -> {
            int compare = item2.getValue().compareTo(item1.getValue());
            if (compare == 0) {
                compare = item1.getKey().compareTo(item2.getKey());
            }
            return compare;
        })).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

    }

    public Map<List<String>, Integer> getCoStarCount(){
        Stream<Movie> movieStream = movies.stream();
        for (int i = 0; i < 500; i++) {
            Movie mv = movies.get(i);
            System.out.println(mv.stars.stream().map(star -> new AbstractMap.SimpleEntry<>(mv.stars.stream().filter(star2 -> !star2.equals(star)).collect(Collectors.toList()), 1)).collect(Collectors.toList()));
        }
        Map<List<String>, Integer> res = movieStream.flatMap(movie -> movie.stars.stream().map(star -> new AbstractMap.SimpleEntry<>(movie.stars.stream().filter(star2 -> !star2.equals(star)).collect(Collectors.toList()), 1))).collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingInt(Map.Entry::getValue))).entrySet().stream().sorted(((item1, item2) -> {
            int compare = item2.getValue().compareTo(item1.getValue());
            if (compare == 0) {
                compare = item1.getKey().toString().compareTo(item2.getKey().toString());
            }
            return compare;
        })).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        return movieStream.flatMap(movie -> movie.stars.stream().map(star -> new AbstractMap.SimpleEntry<>(movie.stars.stream().filter(star2 -> !star2.equals(star)).collect(Collectors.toList()), 1))).collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingInt(Map.Entry::getValue))).entrySet().stream().sorted(((item1, item2) -> {
            int compare = item2.getValue().compareTo(item1.getValue());
            if (compare == 0) {
                compare = item1.getKey().toString().compareTo(item2.getKey().toString());
            }
            return compare;
        })).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
//        return null;
    }

    public List<String> getTopMovies(int top_k, String by){
        return null;
    }

    public List<String> getTopStars(int top_k, String by){
        return null;
    }

    public List<String> searchMovies(String genre, float min_rating, int max_runtime){
        return null;
    }


    public static void main(String[] args) {
        MovieAnalyzer2 movieAnalyzer = new MovieAnalyzer2("resources/imdb_top_500.csv");
        Set<String> genre = new HashSet<>();
        for (int i = 0; i < 500; i++) {
            for (int j = 0; j < movieAnalyzer.movies.get(i).stars.size(); j++) {
                genre.add(movieAnalyzer.movies.get(i).stars.get(j));
            }
        }
        System.out.println(genre);
        System.out.println(movieAnalyzer.getCoStarCount());
    }


}

