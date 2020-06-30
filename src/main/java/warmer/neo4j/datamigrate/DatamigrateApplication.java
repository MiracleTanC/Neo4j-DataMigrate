package warmer.neo4j.datamigrate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import warmer.neo4j.datamigrate.util.Neo4jUtil;

@SpringBootApplication
public class DatamigrateApplication {
    public static void main(String[] args) {
        SpringApplication.run(DatamigrateApplication.class, args);

    }

}
