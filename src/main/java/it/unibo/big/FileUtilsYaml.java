package it.unibo.big;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.BufferedReader;

public class FileUtilsYaml {

	public static JsonNode loadYamlIntoJsonNode(BufferedReader reader){
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		JsonNode jsonNode = null;
		try {
			jsonNode = mapper.readValue(reader, JsonNode.class);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return jsonNode;
	}
}
