package tool.database;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import tool.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataBaseTool {
    private DataBase dataBase;
    private String connectionId;
    public static final String DB_SELECT_URL = "http://localhost:3000/api/query/preview";
    public static final String DB_EXECUTE_URL = "http://localhost:3000/api/output/execute";
    
    @SuppressWarnings("unchecked")
    public RowSetTable select(String sql) throws Exception{
        String requestJSON = "{\"connectionId\": \"" + connectionId + "\", \"sql\": \"" + escapeJson(sql) + "\"}";
        URL url = new URL(DB_SELECT_URL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        connection.setRequestProperty("Accept", "application/json");
        connection.setDoOutput(true);
        try (OutputStream os = connection.getOutputStream()) {
            byte[] inputBytes = requestJSON.getBytes(StandardCharsets.UTF_8);
            os.write(inputBytes, 0, inputBytes.length);
        }
        int responseCode = connection.getResponseCode();
        StringBuilder response = new StringBuilder();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(
                responseCode >= 200 && responseCode < 300 ? connection.getInputStream() : connection.getErrorStream()
        ))) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        }
        String respJson = response.toString();
        JSONObject jsonObject = JSONUtil.parseObj(respJson);
        Map<Object,Object> data = (HashMap<Object, Object>)jsonObject.get("data");
        List<String> columns =(List<String>) data.get("columns");
        RowSetTable table = new RowSetTable(columns);
        List<Map<Object,Object>> rows = (List<Map<Object,Object>>) data.get("rows");
        List<Row> rowList = new ArrayList<>();
        for (String field : table.getField()) {
            rowList.add(new Row());
        }
        for (String field : table.getField()) {
            List<String> valueList = new ArrayList<>();
            for (Map<Object, Object> row : rows) {
                String value = (String) row.get(field);
                valueList.add(value);
            }
            for (int i = 0; i < rowList.size(); i++) {
                rowList.get(i).add(valueList.get(i));
            }
        }
        table.addRow(rowList);
        return table;
    }
    public boolean exec(String sql) throws Exception {
        String requestJSON = "{\"connectionId\": \"" + connectionId + "\", \"sql\": \"" + escapeJson(sql) + "\"}";
        URL url = new URL(DB_EXECUTE_URL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        connection.setRequestProperty("Accept", "application/json");
        connection.setDoOutput(true);
        try (OutputStream os = connection.getOutputStream()) {
            byte[] inputBytes = requestJSON.getBytes(StandardCharsets.UTF_8);
            os.write(inputBytes, 0, inputBytes.length);
        }
        int responseCode = connection.getResponseCode();
        StringBuilder response = new StringBuilder();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(
                responseCode >= 200 && responseCode < 300 ? connection.getInputStream() : connection.getErrorStream()
        ))) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        }
        String respJson = response.toString();
        JSONObject jsonObject = JSONUtil.parseObj(respJson);
        String code =(String) jsonObject.get("code");
        return code.equals("200");
    }
    
    
    
    
    
    private String escapeJson(String input) {
        if (input == null) {
            return "";
        }
        return input.replace("\\", "\\\\")
                    .replace("\"", "\\\"");
    }
    public DataBase getDataBase() {
        return dataBase;
    }
    
    public void setDataBase(DataBase dataBase) {
        this.dataBase = dataBase;
    }
    
    public String getConnectionId() {
        return connectionId;
    }
    
    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }
    
    public DataBaseTool(DataBase dataBase, String connectionId) {
        this.dataBase = dataBase;
        this.connectionId = connectionId;
    }
    
    public DataBaseTool() {
    }
}
