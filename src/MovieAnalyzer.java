import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class MovieAnalyzer {

    public ArrayList<Movie> movies;

    public class Movie {
        public String title;
        public int year;
        public String certificate;
        public int runtime;
        public List<String> genre;
        public float rating;
        public String overview;
        public double metascore;
        public String director;
        public List<String> stars;
        public double votes;
        public double gross;

        public Movie(String title, int year, String certificate, int runtime, List<String> genre,
                     float rating, String overview, double metascore, String director,
                     List<String> stars, double votes, double gross){
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
        public String toString(){
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


    public MovieAnalyzer(String dataset_path){
        movies = new ArrayList<>(500);
        try (BufferedReader br = Files.newBufferedReader(Paths.get(dataset_path),
                StandardCharsets.UTF_8)) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                String res = columns[7];
                if (res.matches("^\".+\"$")) {
                    res = res.substring(1, res.length() - 1);
                }
                Movie movie =
                        new Movie(
                                columns[1].replaceAll("\"", ""),  // FIXME
                                columns[2].isBlank() ? -1 : Integer.parseInt(columns[2]),
                                columns[3],
                                columns[4].isBlank() ? -1 : Integer.parseInt(columns[4].split(" ")[0]),
                                Arrays.asList(columns[5].replace("\"", "").replace(" ", "").split(",")),
                                columns[6].isBlank() ? -1f : Float.parseFloat(columns[6]),
                                res,
                                columns[8].isBlank() ? -1f : Double.parseDouble(columns[8]),
                                columns[9],
                                Arrays.asList(columns[10], columns[11], columns[12], columns[13]),
                                Double.parseDouble(columns[14]), columns.length < 16 ? -1f :
                                Double.parseDouble(columns[15].substring(1, columns[15].length() - 1)
                                        .replaceAll(",", "")));
                movies.add(movie);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public Map<Integer, Integer> getMovieCountByYear(){
        return movies.stream().parallel()
                .collect(Collectors.groupingBy(Movie::getYear, Collectors.summingInt(e -> 1)))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder())).collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1,
                                LinkedHashMap::new));
    }

    public Map<String, Integer> getMovieCountByGenre(){
        return movies.stream().parallel()
                .flatMap(movie -> movie.genre.stream())
                .collect(Collectors.groupingBy(e -> e, Collectors.summingInt(e -> 1))).entrySet()
                .stream().sorted(((item1, item2) -> {
                    int compare = item2.getValue().compareTo(item1.getValue());
                    if (compare == 0) {
                        compare = item1.getKey().compareTo(item2.getKey());
                    }
                    return compare;
                })).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1,
                        LinkedHashMap::new));
    }

    public Map<List<String>, Integer> getCoStarCount(){
        return movies.stream().parallel()
                .flatMap(m -> {
                    List<List<String>> res = new LinkedList<>();
                    for (int i = 0; i < m.stars.size(); i++) {
                        for (int j = i + 1; j < m.stars.size(); j++) {
                            List<String> ss = new LinkedList<>(List.of(m.stars.get(i), m.stars.get(j)));
                            ss.sort(String::compareTo);
                            res.add(ss);
                        }
                    }
                    return res.stream();
                })
                .collect(Collectors.toMap(
                        ss -> ss,
                        ss -> 1,
                        Integer::sum
                ));
    }

    public List<String> getTopMovies(int top_k, String by){
        Comparator<Movie> comp = (m1, m2) -> {
            int res = "runtime".equals(by)
                    ? Integer.compare(m2.runtime, m1.runtime)
                    : Integer.compare(m2.overview.length(), m1.overview.length());
            if (res != 0) {
                return res;
            }
            return m1.title.compareTo(m2.title);
        };
        return movies.stream()
                .sorted(comp)
                .map(m -> m.title)
                .limit(top_k)
                .toList();
    }

    public List<String> getTopStars(int top_k, String by){
        Map<String, Integer> res0 = new HashMap<>();
        Map<String, Double> res = new HashMap<>();

        movies.stream()
                .flatMap(m -> m.stars.stream()
                        .map(s -> List.of(s, "rating".equals(by) ? m.rating : m.gross)))
                .filter(m -> (Double) m.get(1) != -1)
                .forEach(e -> {
                    String name = (String) e.get(0);
//                    float val = (float)((Double) e.get(1)).doubleValue();
                    Double val = (Double) e.get(1);
//                    System.out.println(name + " " + val);
                    if (!res0.containsKey(name)) {
                        res0.put(name, 1);
                    } else {
                        res0.put(name, res0.get(name) + 1);
                    }
                    if (!res.containsKey(name)) {
                        res.put(name, val);
                    } else {
//                        System.out.println(name + " 1 "+ res.get(name) + " 22 " + (float)res.get(name).floatValue() + " 33 "+((float)res.get(name).floatValue() + val));
                        res.put(name, res.get(name) + val);
//                        System.out.println(name + " 1 "+ res.get(name) + " 2 " + res.get(name).floatValue());
                    }
                });

        return res.entrySet().stream().parallel()
                .sorted((e1, e2) -> {
                    double v1 = 0;
                    double v2 = 0;
                    if (by.equals("rating")) {
                        v1 = ( (double) e1.getValue() / res0.get(e1.getKey()));
                        v2 = ( (double) e2.getValue() / res0.get(e2.getKey()));
                    }else{
                        v1 = (e1.getValue() / res0.get(e1.getKey()));
                        v2 = (e2.getValue() / res0.get(e2.getKey()));
                    }

//                int res2 = Double.compare(v2, v1);
//                if (res2 != 0) {
//                    return res2;
//                }
//                    if (by.equals("rating"))
//                        System.out.println(e1.getKey() + " " + v1 + ", " + e2.getKey() + " " + v2);
                    if (v2 != v1) {
                        return (v2 - v1)>0?1:-1;
                    }
                    return e1.getKey().compareTo(e2.getKey());
                })
                .limit(top_k)
                .map(Map.Entry::getKey)
                .toList();
    }

    public List<String> searchMovies(String genre, float min_rating, int max_runtime){
        return movies.stream().parallel()
                .filter(m -> m.genre.contains(genre))
                .filter(m -> m.rating >= min_rating)
                .filter(m -> m.runtime <= max_runtime)
                .map(m -> m.title)
                .sorted(String::compareTo)
                .toList();
    }

    public static void main(String[] args){
        String a = "8.8";
        String b = "8.7";
        String c = "8.9";
        double aa = Double.parseDouble(a);
        double bb = Double.parseDouble(b);
        double cc = Double.parseDouble(c);

        double m = (aa+bb+cc)/3.0;
        System.out.println(aa);
        System.out.println(m);

        float a1 = 8.9f;
        float a2 = 8.8f;
        float a3 = 8.7f;

        float a4 = (a1+a2+a3)/3;
        System.out.println(a4);

        MovieAnalyzer ma = new MovieAnalyzer("resources/imdb_top_500.csv");
        ma.getTopStars(15,"rating");

    }

}
