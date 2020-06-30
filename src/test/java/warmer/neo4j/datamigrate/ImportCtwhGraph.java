package warmer.neo4j.datamigrate;

import com.alibaba.fastjson.JSON;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.driver.v1.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import warmer.neo4j.datamigrate.builder.DataMigrateUtil;
import warmer.neo4j.datamigrate.util.Neo4jUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
@WebAppConfiguration
public class ImportCtwhGraph {
    @Autowired
    private DataMigrateUtil dataMigrateUtil;

    @Value("${migrate.cypher.path}")
    private  String cypherPath;

    private final String sourceDomain = "zhctwh";//源领域标签
    private final String targetDomain = "ctwh";//目标领域标签

    /**
     * 迁移的目标数据库
     */
    private final String targetNeo4jUrl = "bolt://127.0.0.1:7687";
    private final String targetNeo4jUserName = "neo4j";
    private final String targetNeo4jPassword = "123456";

    @Test
    public void dataMigrate() throws IOException {
        //生成节点脚本
        //dataMigrateUtil.handNodesData(sourceDomain, targetDomain,cypherPath);
        //生成关系脚本
        //dataMigrateUtil.handShipData(sourceDomain, targetDomain,cypherPath);
        //执行节点脚本
        String nodeCypherFileName=String.format("%s\\%s",cypherPath,"neo4jNodesCypher.txt");
        dataMigrateUtil.runCypherScriptFormFile(nodeCypherFileName,targetNeo4jUrl,targetNeo4jUserName,targetNeo4jPassword);
        //执行关系脚本
        String shipCypherFileName=String.format("%s\\%s",cypherPath,"neo4jShipsCypher.txt");
        //dataMigrateUtil.runCypherScriptFormFile(shipCypherFileName,targetNeo4jUrl,targetNeo4jUserName,targetNeo4jPassword);
        System.out.println("success");
    }

}
