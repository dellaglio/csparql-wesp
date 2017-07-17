package org.streamreasoning.wsp.csparql.config;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.BasePathLocationStrategy;
import org.apache.commons.configuration2.io.ClasspathLocationStrategy;
import org.apache.commons.configuration2.io.CombinedLocationStrategy;
import org.apache.commons.configuration2.io.FileLocationStrategy;
import org.apache.commons.configuration2.io.FileSystemLocationStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
	public static final Config INSTANCE = new Config();

	private final Logger logger = LoggerFactory.getLogger(Config.class);

	private Configuration config;

	private Config() {
		try {
			List<FileLocationStrategy> subs = Arrays.asList(
					new BasePathLocationStrategy(),
					new FileSystemLocationStrategy(),
					new ClasspathLocationStrategy());
			
			FileLocationStrategy strategy = new CombinedLocationStrategy(subs);
			
			FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class);
			Parameters params = new Parameters();
			
			builder.configure(params.fileBased()
					.setFileName("wsp.properties")
					.setLocationStrategy(strategy)
					);
			
			config = builder.getConfiguration();
			logger.debug("Configuration file successfully lodead");
		} catch (ConfigurationException e) {
			logger.error("Error while lading the configuration file; default config will be used", e);
			config = new BaseConfiguration();
			config.addProperty("mqtt.enabled", false);
			config.addProperty("ws.enabled", false);
		}
	}

	public String getServerUrl(){
		return "http://"+getServerIp()+":"+getServerPort();
	}
	
	public Integer getServerPort(){
		return config.getInt("server.port");
	}
	
	public String getServerIp(){
		return config.getString("server.ip");
	}
	
	public boolean isMQTTEnabled() {
		return config.getBoolean("mqtt.enabled");
	}

	public boolean isWSEnabled() {
		return config.getBoolean("ws.enabled");
	}

	public String getMQTTBrokerUrl() {
		return config.getString("mqtt.broker.url");
	}
	
	//mainly for test purposes
	public void setConfigParams(Properties properties){
		for(Entry<Object,Object> entry : properties.entrySet())
			config.setProperty(entry.getKey().toString(), entry.getValue());
	}
	
}
