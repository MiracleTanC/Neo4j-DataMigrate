package warmer.neo4j.datamigrate.builder;

import com.alibaba.fastjson.JSON;
import org.neo4j.driver.v1.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import warmer.neo4j.datamigrate.util.Neo4jUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@Component
public class DataMigrateUtil {


    @Autowired
    Neo4jUtil neo4jUtil;

    /**neo4j 报OutofMemoryError解决方法,参考下面文档
     * https://neo4j.com/docs/operations-manual/current/performance/memory-configuration/#heap-sizing
     * 在windows环境下，在cmd窗口运行neo4j-import命令前，先设置一个环境变量，
     * 比如设置为4G set HEAP_SIZE=4096m
     * 修改 neo4j.conf 中的配置 (docker 中默认为512m)
     * dbms.memory.heap.initial_size=2048m
     * dbms.memory.heap.max_size=4096m
     * @throws IOException
     */

    /**
     * 处理节点数据
     */
    public void handNodesData(String sourceDomain,String targetDomain,String cypherPath){
        System.out.println("==========读取节点信息===========");
        String cyphertotal = String.format("match(n:`%s`)  return count(n)", sourceDomain);
        long total = neo4jUtil.GetGraphValue(cyphertotal);
        Integer pageSize = 500;
        long toatlPage = total / pageSize + ((total % pageSize) == 0 ? 0 : 1);
        System.out.println(String.format("==========共【%s】个节点,分【%s】次写入脚本===========",total,toatlPage));
        StringBuilder builder = new StringBuilder();
        for (Integer pageIndex = 0; pageIndex < toatlPage; pageIndex++) {
            System.out.println(String.format("==========【%s/%s】===========",pageIndex,toatlPage));
            String cypher = String.format("match(n:`%s`)  return n skip %s limit %s", sourceDomain, pageSize * pageIndex, pageSize);
            List<HashMap<String, Object>> nodes = neo4jUtil.GetGraphNode(cypher);
            String text = batchCreateNodeCypher(targetDomain, nodes);
            //System.out.println(text);
            //neo4jUtil.excuteCypherSql(text);
            //这里为啥不直接执行脚本?考虑到内网的问题，先把脚本保存起来，传到内网服务器执行
            builder.append(text);
            builder.append("\r\n");
            builder.append("=============================");
            builder.append("\r\n");
        }
        String text = builder.toString();
        String fileName=String.format("%s\\%s",cypherPath,"neo4jNodesCypher.txt");
        buildFile(fileName,text);
        System.out.println(String.format("===========生成节点脚本完毕，路径【%s】============",fileName));
    }
    /**
     * 处理关系数据
     */
    public void handShipData(String sourceDomain,String targetDomain,String cypherPath) {
        String cyphertotal = String.format("match(n:`%s`)-[r]->(m)  return count(r)", sourceDomain);
        long total = neo4jUtil.GetGraphValue(cyphertotal);
        System.out.println(total);
        Integer pageSize = 500;
        long toatlPage = total / pageSize + ((total % pageSize) == 0 ? 0 : 1);
        System.out.println(String.format("==========共【%s】个节点,分【%s】次写入脚本===========",total,toatlPage));
        StringBuilder builder = new StringBuilder();
        for (Integer pageIndex = 0; pageIndex < toatlPage; pageIndex++) {
            System.out.println(String.format("==========【%s/%s】===========",pageIndex,toatlPage));
            String cypher = String.format("match(n:`%s`)-[r]->(m)  return r skip %s limit %s", sourceDomain, pageSize * pageIndex, pageSize);
            List<HashMap<String, Object>> ships = neo4jUtil.GetGraphRelationShip(cypher);
            String text = batchCreateRelationCypher(targetDomain, ships);
            System.out.println(text);
            builder.append(text);
            builder.append("\r\n");
            builder.append("=============================");
            builder.append("\r\n");
        }
        String text = builder.toString();
        String fileName=String.format("%s\\%s",cypherPath,"neo4jShipsCypher.txt");
        buildFile(fileName,text);
        System.out.println(String.format("===========生成关系脚本完毕，路径【%s】============",fileName));
    }
    /**
     * 生成文本
     *
     * @param path
     * @param text
     */
    private void buildFile(String path, String text) {
        File file = new File(path);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
        try {
            file.createNewFile();
            OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(text);
            bw.flush();
            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量生成节点脚本
     * @param domain
     * @param nodes
     * @return
     */
    private String batchCreateNodeCypher(String domain, List<HashMap<String, Object>> nodes) {
        String cypher = "";
        String data = "";
        List<String> cyList = new ArrayList<>();
        List<String> nodeKeys = new ArrayList<>();
        int i = 0;
        for (HashMap<String, Object> node : nodes) {
            String json = JSON.toJSONString(node);
            String propertiesString = neo4jUtil.getFilterPropertiesJson(json);
            cyList.add(propertiesString);
            if (i == 0) {
                nodeKeys = node.keySet().stream().map(n -> String.format("%s:row.%s", n, n)).collect(Collectors.toList());
            }
            i++;
        }
        if (cyList != null && cyList.size() > 0) {
            data = String.join(",", cyList);
        }
        String batchData = String.format(" [%s]", data);
        String propStr = String.format("{%s}", String.join(",", nodeKeys));
        cypher = String.format(
                "UNWIND %s as row  \r\n" +
                        "Create (n:`%s` %s);", batchData, domain, propStr);

        return cypher;
    }

    /**
     * 批量生成neo4jbroswer可执行的脚本
     * @param domain
     * @param nodes
     * @return
     */
    private String batchCreateNodeCypherForBroswer(String domain, List<HashMap<String, Object>> nodes) {
        String cypher = "";
        String data = "";
        List<String> cyList = new ArrayList<>();
        List<String> nodeKeys = new ArrayList<>();
        int i = 0;
        for (HashMap<String, Object> node : nodes) {
            String json = JSON.toJSONString(node);
            String propertiesString = neo4jUtil.getFilterPropertiesJson(json);
            cyList.add(propertiesString);
            if (i == 0) {
                nodeKeys = node.keySet().stream().map(n -> String.format("%s:row.%s", n, n)).collect(Collectors.toList());
            }
            i++;
        }
        if (cyList != null && cyList.size() > 0) {
            data = String.join(",", cyList);
        }
        String batchData = String.format(":param batchData: [%s];", data);
        neo4jUtil.excuteCypherSql(batchData);
        String propStr = String.format("{%s}", String.join(",", nodeKeys));
        cypher = String.format(
                "%s \r\n" +
                        "UNWIND $batchData as row  \r\n" +
                        "Create (n:`%s` %s);", batchData, domain, propStr);
        neo4jUtil.excuteCypherSql(cypher);
        return cypher;
    }

    /**
     * 批量生成关系脚本
     * @param domain
     * @param ships
     * @return
     */
    private String batchCreateRelationCypher(String domain, List<HashMap<String, Object>> ships) {
        String cypher = "";
        String data = "";
        List<String> cyList = new ArrayList<>();
        List<String> shipKeys = new ArrayList<>();
        int i = 0;
        for (HashMap<String, Object> ship : ships) {
            String json = JSON.toJSONString(ship);
            String propertiesString = neo4jUtil.getFilterPropertiesJson(json);
            cyList.add(propertiesString);
            if (i == 0) {
                shipKeys = ship.keySet().stream().map(n -> String.format("%s:row.%s", n, n)).collect(Collectors.toList());
            }
            i++;
        }
        if (cyList != null && cyList.size() > 0) {
            data = String.join(",", cyList);
        }
        String batchData = String.format(" [%s]", data);
        String propStr = String.format("{%s}", String.join(",", shipKeys));
        cypher = String.format(
                "UNWIND %s as row  \r\n" +
                        "MATCH (n:`%s`),(m:`%s`)\r\n" +
                        "where n.uuid=row.sourceid and m.uuid=row.targetid \r\n" +
                        "create (n)-[r:RE %s]->(m)\r\n", batchData, domain, domain, propStr);
        return cypher;
    }



    /**
     * 执行生成的脚本
     * @param path
     * @throws IOException
     */
    public void runCypherScriptFormFile(String path,String url,String userName,String password) throws IOException {
        File file = new File(path);
        if(!file.exists()){
            System.out.println("脚本不存在");
            return;
        }
        InputStream in = null;
        BufferedReader br = null;
        try {
            in = new FileInputStream(file);
            br = new BufferedReader(new InputStreamReader(in));
            String s;
            String cypher="";
            while ((s = br.readLine())!=null) {
                if(!s.equals("=============================")){
                    cypher+=s;
                    cypher+="\r\n";
                }else{
                    excuteCypherSql(cypher,url,userName,password);
                    cypher="";
                }
            }
        } catch (Exception ex) {
            throw ex;
        }
    }
    private StatementResult excuteCypherSql(String cypherSql, String url, String userName, String password){
        Driver neo4jDriver = GraphDatabase.driver(url, AuthTokens.basic(userName, password));
        StatementResult result = null;
        try (Session session = neo4jDriver.session()) {
            System.out.println(cypherSql);
            result = session.run(cypherSql);
        } catch (Exception e) {
            throw e;
        }
        return result;
    }
}
