import java.sql.*;
import java.util.Locale;

public class PostgreSQLJDBC {

    public static void main(String args[]) {
        //String jdbcURL = "jdbc:postgresql://localhost:5432/soax95";
        String jdbcURL = "jdbc:postgresql://ls-41d379b19b475ed294babb170cfa0f93b3011e47.cq2f1e9koedo.us-east-2.rds.amazonaws.com/dvdrental";
        String username = "dbmasteruser";
        String password = "Swnp3XQFtBd)b61NGn!uh{Lw=8#Vk~y<";

        try {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(jdbcURL, username, password);
            // connection.setAutoCommit(false);
            System.out.println("Connected to PostgreSQL Server");
            Statement stmt = connection.createStatement();
            ResultSet result = stmt.executeQuery("SELECT f.film_id, title, description, name AS category, release_year, language_id, rental_duration, rental_rate, length, rating, \n" +
                    "f.last_update, special_features, fulltext, concat(first_name, ' ', last_name) AS actor_name\n" +
                    "FROM film AS f\n" +
                    "INNER JOIN film_actor AS fa\n" +
                    "ON f.film_id = fa.film_id\n" +
                    "INNER JOIN actor AS a\n" +
                    "ON fa.actor_id = a.actor_id\n" +
                    "INNER JOIN film_category AS fc\n" +
                    "ON fa.film_id = fc.film_id\n" +
                    "INNER JOIN category AS c\n" +
                    "ON fc.category_id = c.category_id\n" +
                    "LIMIT 250 ");

            while (result.next()) {
                int film_id = result.getInt("film_id");
                String title = result.getString("title").toUpperCase(Locale.ROOT);
                String description = result.getString("description");
                String category = result.getString("category");
                if (category.substring(0,1).equalsIgnoreCase("D")){
                    category = result.getString("category").toUpperCase(Locale.ROOT);
                }
                else if (category.substring(0,1).equalsIgnoreCase("N")) {
                    category = result.getString("category").toLowerCase(Locale.ROOT);
                }
                int release_year = result.getInt("release_year");
                int language_id = result.getInt("language_id");
                int rental_duration = result.getInt("rental_duration");
                float rental_rate = result.getFloat("rental_rate");
                int length = result.getInt("length");
                String rating = result.getString("rating");
                Timestamp last_update = result.getTimestamp("last_update");
                Array special_features = result.getArray("special_features");
                String fullText = result.getString("fulltext");
                String actor_name = result.getString("actor_name");


                System.out.println("(film_id: " + film_id + ") " + "(title: " + title + ") " + "(description: "+ description + ") " +
                        "(category: " + category + ") " + "(release_year: " + release_year + ") " + "(language_id: " + language_id +
                        ") " + "(rental_duration: " + rental_duration + ") " + "(rental_rate: " + rental_rate + ") " +
                        "(length: " + length + ") " + "(rating: " + rating + ") " + "(last_update: " + last_update + ") " +
                        "(special_features: " + special_features + ") " + "(fulltext: " + fullText + ") " +
                        "(actor_name: " + actor_name + ") ");

            }


            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error in connecting to PostgreSQL Server");

        }
    }
}

// COMMANDS TO RUN JAVA CODE
/*
--clean compile code
mvn clean compile

--sets environment variable for JAVA--
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.12.jdk/Contents/Home

--runs main class
mvn compile exec:java -Dexec.mainClass=PostgreSQLJDBC
*/