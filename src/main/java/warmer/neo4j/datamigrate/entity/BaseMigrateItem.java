package warmer.neo4j.datamigrate.entity;

import lombok.Data;

@Data
public class BaseMigrateItem {
    private final String nodeCypherPath = "D:\\text\\ctwhcypher.sql";
    private final String relationCypherPath = "D:\\text\\ctwhrelationcypher.sql";
    private final String sourceDomain;//源领域标签
    private final String targetDomain;//目标领域标签

    /**
     * 迁移的目标数据库
     */
    private final String targetNeo4jUrl="bolt://127.0.0.1:7687";
    private final String targetNeo4jUserName="neo4j";
    private final String targetNeo4jPassword="123456";
}
