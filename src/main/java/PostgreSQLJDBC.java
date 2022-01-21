
import com.google.cloud.bigquery.*;
import java.sql.*;
import java.util.Locale;
import java.util.Scanner;


public class PostgreSQLJDBC {

    public static void main(String args[]) {
        // credentials for connecting to pgadmin
        String jdbcURL = "jdbc:postgresql://ls-41d379b19b475ed294babb170cfa0f93b3011e47.cq2f1e9koedo.us-east-2.rds.amazonaws.com/dvdrental";
        String username = "dbmasteruser";
        String password = "Swnp3XQFtBd)b61NGn!uh{Lw=8#Vk~y<";

        // Setting up for User input
        Scanner input = new Scanner(System.in);  // create scanner object
        System.out.println("Enter a name");

        String inputName = input.nextLine(); // Read user input


        // connect to BigQuery
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("york-cdf-start").build().getService();

        try {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(jdbcURL, username, password);
            System.out.println("Connected to PostgreSQL Server");
            Statement stmt = connection.createStatement();
            ResultSet result = stmt.executeQuery("SELECT f.film_id, title, description, name AS category, release_year, language_id, \n" +
                    "rental_duration, rental_rate, length, rating, f.last_update, special_features, fulltext, concat(first_name, ' ', last_name) AS actor_name\n" +
                    "FROM film AS f\n" +
                    "INNER JOIN film_actor AS fa\n" +
                    "ON f.film_id = fa.film_id\n" +
                    "INNER JOIN actor AS a\n" +
                    "ON fa.actor_id = a.actor_id\n" +
                    "INNER JOIN film_category AS fc\n" +
                    "ON fa.film_id = fc.film_id\n" +
                    "INNER JOIN category AS c\n" +
                    "ON fc.category_id = c.category_id\n" +
                    "WHERE first_name LIKE " + " '" +inputName + "' " + " OR last_name LIKE " + " '" + inputName +"' \n " +
                    "LIMIT 10 ");


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


                // Prepare query job
                final String INSERT_DATA =
                        "INSERT INTO `york-cdf-start.final_soa_xiong.java_etl_application` " +
                                "(film_id, title, description, category, release_year, language_id, " +
                                "rental_duration, rental_rate, length, rating, last_update, special_features, " +
                                "fulltext, actor_name) " +
                                "VALUES (" + film_id + ", '" + title + "', '" + description + "', '" + category +
                                "', " + release_year + ", " + language_id + ", " + rental_duration +
                                ", " + rental_rate + ", " + length + ", '" + rating + "', '" + last_update + "', '" +
                                special_features + "', \"" + fullText + "\", '" + actor_name + "');";
                QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(INSERT_DATA).build();

                // Run the job on BigQuery
                Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
                queryJob = queryJob.waitFor();
                if (queryJob == null) {
                    throw new Exception("job no longer exists");
                }
                // once the job is done, check if any error occurred
                if (queryJob.getStatus().getError() != null) {
                    throw new Exception(queryJob.getStatus().getError().toString());
                }

                // Display results
                // Here, we will print the total number of rows that were inserted
                JobStatistics.QueryStatistics stats = queryJob.getStatistics();
                Long rowsInserted = stats.getDmlStats().getInsertedRowCount();
                System.out.printf("%d rows inserted\n", rowsInserted);



                /*
                System.out.println("(film_id: " + film_id + ") " + "(title: " + title + ") " + "(description: "+ description + ") " +
                        "(category: " + category + ") " + "(release_year: " + release_year + ") " + "(language_id: " + language_id +
                        ") " + "(rental_duration: " + rental_duration + ") " + "(rental_rate: " + rental_rate + ") " +
                        "(length: " + length + ") " + "(rating: " + rating + ") " + "(last_update: " + last_update + ") " +
                        "(special_features: " + special_features + ") " + "(fulltext: " + fullText + ") " +
                        "(actor_name: " + actor_name + ") ");
                 */

            }

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error in connecting to PostgreSQL Server");

        }
    }
}


// table schema
        /*
        TableSchema table_schema = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("film_id").setType("INTEGER").setMode("NULLABLE"),
                new TableFieldSchema().setName("title").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("description").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("category").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("release_year").setType("INTEGER").setMode("NULLABLE"),
                new TableFieldSchema().setName("language_id").setType("INTEGER").setMode("NULLABLE"),
                new TableFieldSchema().setName("rental_duration").setType("INTEGER").setMode("NULLABLE"),
                new TableFieldSchema().setName("rental_rate").setType("FLOAT").setMode("NULLABLE"),
                new TableFieldSchema().setName("length").setType("INTEGER").setMode("NULLABLE"),
                new TableFieldSchema().setName("rating").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("last_update").setType("TIMESTAMP").setMode("NULLABLE"),
                new TableFieldSchema().setName("special_features").setType("TEXT").setMode("NULLABLE"),
                new TableFieldSchema().setName("fulltext").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("actor_name").setType("STRING").setMode("NULLABLE")));

         */

/*
    final String INSERT_FILM =
            "INSERT INTO `york-cdf-start.final_soa-xiong.java_etl_application` (film_id, title, name, " +
                    "description, release_year, language_id, rental_duration, rental_rate, length, " +
                    "replacement_cost, rating, last_update, special_features, fulltext, first_name, " +
                    "last_name) VALUES (" + film_id + ", '" + title + "', '" + name + "', '" + description +
                    "', " + release_year + ", " + language_id + ", " + rental_duration + ", " + rental_rate +
                    ", " + length + ", " + replacement_cost + ", '" + rating + "', '" + last_update + "', '" +
                    special_features + "', \"" + fulltext + "\", '" + first_name + "', '" + last_name + "');";

    QueryJobConfiguration queryConfig =
            QueryJobConfiguration.newBuilder(INSERT_FILM).build();

    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
                queryJob = queryJob.waitFor();
                        if (queryJob == null) {
                        throw new Exception("job no longer exists");
                        }
                        // once the job is done, check if any error occured
                        if (queryJob.getStatus().getError() != null) {
                        throw new Exception(queryJob.getStatus().getError().toString());
                        }

                        JobStatistics.QueryStatistics stats = queryJob.getStatistics();
                        Long rowsInserted = stats.getDmlStats().getInsertedRowCount();
                        System.out.printf("%d rows inserted\n", rowsInserted);

 */


// COMMANDS TO RUN JAVA CODE
/*
--clean compile code
mvn clean compile

-- GOOGLE APPLICATION CREDENTIALS
export GOOGLE_APPLICATION_CREDENTIALS="/Users/yorkmac048/IdeaProjects/java_etl_application/src/main/york-cdf-start-8a26c05b158d.json"

--sets environment variable for JAVA--
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.12.jdk/Contents/Home

--runs main class
mvn compile exec:java -Dexec.mainClass=PostgreSQLJDBC
*/