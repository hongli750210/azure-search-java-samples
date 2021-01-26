package main.java.app;

import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.Context;
import com.azure.search.documents.SearchClient;
import com.azure.search.documents.SearchServiceVersion;
import com.azure.search.documents.indexes.SearchIndexClient;
import com.azure.search.documents.indexes.SearchIndexClientBuilder;
import com.azure.search.documents.indexes.models.LexicalAnalyzerName;
import com.azure.search.documents.indexes.models.SearchField;
import com.azure.search.documents.indexes.models.SearchFieldDataType;
import com.azure.search.documents.indexes.models.SearchIndex;
import com.azure.search.documents.models.SearchOptions;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;

public class App {

    private static Properties loadPropertiesFromResource(String resourcePath) throws IOException {
        InputStream inputStream = App.class.getResourceAsStream(resourcePath);
        var configProperties = new Properties();
        configProperties.load(inputStream);
        return configProperties;
    }

    private static JSONObject loadJsonObjectFromResource(String resourcePath) {
        InputStream inputStream = App.class.getResourceAsStream(resourcePath);
        return new JSONObject(new JSONTokener(inputStream));
    }

    private static List<Map<String, Object>> createDocumentMap(JSONArray dcoJson) {
        List<Map<String, Object>> documentMapList = new ArrayList<>();
        for (int i = 0; i < dcoJson.length(); i++) {
            JSONObject docObject = dcoJson.getJSONObject(i);
            Map<String, Object> docMap = new HashMap<>();
            documentMapList.add(docMap);
            for (Iterator<String> iterator = docObject.keys(); iterator.hasNext(); ) {
                String strKey = iterator.next();
                switch (strKey) {
                    case "@search.action":
                    case "HotelId":
                    case "HotelName":
                    case "Description":
                    case "Description_fr":
                    case "Category":
                        docMap.put(strKey, docObject.getString(strKey));
                        break;
                    case "Tags":
                        List<String> tagList = new ArrayList<>();
                        docMap.put(strKey, tagList);
                        JSONArray tagsArray = docObject.getJSONArray(strKey);
                        for (int j = 0; j < tagsArray.length(); j++) {
                            tagList.add(tagsArray.getString(j));
                        }
                        break;
                    case "ParkingIncluded":
                        docMap.put(strKey, docObject.getBoolean(strKey));
                        break;
                    case "LastRenovationDate":
                        docMap.put(strKey, Instant.parse(docObject.getString(strKey)));
                        break;
                    case "Rating":
                        docMap.put(strKey, docObject.getDouble(strKey));
                        break;
                    case "Address":
                        docMap.put(strKey, docObject.getJSONObject(strKey).toMap());
                }
            }
        }
        return documentMapList;
    }

    private static List<SearchField> createIndexFields(JSONArray fieldsJson) {
        List<SearchField> searchFieldList = new ArrayList<>();
        for (int i = 0; i < fieldsJson.length(); i++) {
            JSONObject fieldObject = fieldsJson.getJSONObject(i);
            SearchField searchField = new SearchField(fieldObject.getString("name"), SearchFieldDataType.fromString(fieldObject.getString("type")));
            for (Iterator<String> iterator = fieldObject.keys(); iterator.hasNext(); ) {
                String strKey = iterator.next();
                switch (strKey) {
                    case "key":
                        searchField.setKey(fieldObject.getBoolean(strKey));
                        break;
                    case "filterable":
                        searchField.setFilterable(fieldObject.getBoolean(strKey));
                        break;
                    case "searchable":
                        searchField.setSearchable(fieldObject.getBoolean(strKey));
                        break;
                    case "sortable":
                        searchField.setSortable(fieldObject.getBoolean(strKey));
                        break;
                    case "facetable":
                        searchField.setFacetable(fieldObject.getBoolean(strKey));
                        break;
                    case "analyzer":
                        searchField.setAnalyzerName(LexicalAnalyzerName.fromString(fieldObject.getString(strKey)));
                        break;
                    case "fields":
                        searchField.setFields(createIndexFields(fieldObject.getJSONArray(strKey)));
                        break;
                    default:
                        break;
                }
            }
            searchFieldList.add(searchField);
        }
        return searchFieldList;
    }

    public static void main(String[] args) {
        try {
            Properties config = loadPropertiesFromResource("/app/config.properties");
            SearchIndexClient indexClient = new SearchIndexClientBuilder()
                    .endpoint(String.format(config.getProperty("SearchServiceEndPoint"),config.getProperty("SearchServiceName")))
                    .credential(new AzureKeyCredential(config.getProperty("SearchServiceAdminKey")))
                    .serviceVersion(SearchServiceVersion.getLatest())
                    .buildClient();



//Uncomment the next 3 lines in the 1 - Create Index section of the quickstart
            JSONObject indexJson = loadJsonObjectFromResource("/service/index.json");
            SearchIndex searchIndex = new SearchIndex(indexJson.getString("name"));
            JSONArray fields = indexJson.getJSONArray("fields");
            searchIndex.setFields(createIndexFields(fields));
            indexClient.createOrUpdateIndex(searchIndex);
            SearchClient client = indexClient.getSearchClient(searchIndex.getName());
            Thread.sleep(1000L); // wait a second to create the index

//Uncomment the next 2 lines in the 2 - Load Documents section of the quickstart
            JSONObject documentJson = loadJsonObjectFromResource("/service/hotels.json");
            client.mergeOrUploadDocuments(createDocumentMap(documentJson.getJSONArray("value")));
            Thread.sleep(2000L); // wait 2 seconds for data to upload

//Uncomment the following 5 search queries in the 3 - Search an index section of the quickstart
            // Query 1
            System.out.println("\n*QUERY 1****************************************************************");
            System.out.println("Search for: Atlanta");
            System.out.println("Return: All fields");
            client.search("Atlanta").forEach(searchResult -> {
                System.out.println("QUERY 1 search result*************************************************");
                Map<String,Object> resultMap = searchResult.getDocument(Map.class);
                for ( Iterator<String> iterator = resultMap.keySet().iterator(); iterator.hasNext(); ) {
                    String key = iterator.next();
                    System.out.println(String.format("%s : %s", key , resultMap.get(key)));
                }
            });

            // Query 2
            System.out.println("\n*QUERY 2****************************************************************");
            System.out.println("Search for: Atlanta");
            System.out.println("Return: HotelName, Tags, Address");
            SearchOptions searchOptions = new SearchOptions();
            searchOptions.setSelect("HotelName", "Tags", "Address");
            client.search("Atlanta", searchOptions, new Context("key1","value1")).forEach(searchResult -> {
                System.out.println("QUERY 2 search result*************************************************");
                Map<String,Object> resultMap = searchResult.getDocument(Map.class);
                for ( Iterator<String> iterator = resultMap.keySet().iterator(); iterator.hasNext(); ) {
                    String key = iterator.next();
                    System.out.println(String.format("%s : %s", key , resultMap.get(key)));
                }
            });

            //Query 3
            System.out.println("\n*QUERY 3****************************************************************");
            System.out.println("Search for: wifi & restaurant");
            System.out.println("Return: HotelName, Description, Tags");
            searchOptions = new SearchOptions();
            searchOptions.setSelect("HotelName", "Description", "Tags");
            client.search("wifi,restaurant", searchOptions, new Context("key1","value1")).forEach(searchResult -> {
                System.out.println("QUERY 3 search result*************************************************");
                Map<String,Object> resultMap = searchResult.getDocument(Map.class);
                for ( Iterator<String> iterator = resultMap.keySet().iterator(); iterator.hasNext(); ) {
                    String key = iterator.next();
                    System.out.println(String.format("%s : %s", key , resultMap.get(key)));
                }
            });

            // Query 4 -filtered query
            System.out.println("\n*QUERY 4****************************************************************");
            System.out.println("Search for: all");
            System.out.println("Filter: Ratings greater than 4");
            System.out.println("Return: HotelName, Rating");
            searchOptions = new SearchOptions();
            searchOptions.setSelect("HotelName", "Rating");
            searchOptions.setFilter("Rating gt 4");
            client.search("*",searchOptions, new Context("key1","value1")).forEach(searchResult -> {
                System.out.println("QUERY 4 search result*************************************************");
                Map<String,Object> resultMap = searchResult.getDocument(Map.class);
                for ( Iterator<String> iterator = resultMap.keySet().iterator(); iterator.hasNext(); ) {
                    String key = iterator.next();
                    System.out.println(String.format("%s : %s", key , resultMap.get(key)));
                }
            });

            // Query 5 - top 2 results, ordered by
            System.out.println("\n*QUERY 5****************************************************************");
            System.out.println("Search for: boutique");
            System.out.println("Get: Top 2 results");
            System.out.println("Order by: Rating in descending order");
            System.out.println("Return: HotelId, HotelName, Category, Rating");
            searchOptions = new SearchOptions();
            searchOptions.setSelect("HotelId", "HotelName", "Category", "Rating");
            searchOptions.setOrderBy("Rating desc");
            searchOptions.setTop(2);
            client.search("boutique", searchOptions, new Context("key1","value1")).forEach(searchResult -> {
                System.out.println("QUERY 5 search result*************************************************");
                Map<String,Object> resultMap = searchResult.getDocument(Map.class);
                for ( Iterator<String> iterator = resultMap.keySet().iterator(); iterator.hasNext(); ) {
                    String key = iterator.next();
                    System.out.println(String.format("%s : %s", key , resultMap.get(key)));
                }
            });

        } catch (Exception e) {
            System.err.println("Exception:" + e.getMessage());
            e.printStackTrace();
        }
    }
}