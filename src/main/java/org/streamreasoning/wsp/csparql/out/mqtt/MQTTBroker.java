package org.streamreasoning.wsp.csparql.out.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.streamreasoning.wsp.csparql.config.Config;

public class MQTTBroker {
	public static final MQTTBroker INSTANCE = new MQTTBroker();

	private MqttClient broker;
	private String brokerUrl;
	
	private MQTTBroker(){
		System.out.println("init class");
		brokerUrl = Config.INSTANCE.getMQTTBrokerUrl();
		MemoryPersistence pers = new MemoryPersistence();
		try {
			broker = new MqttClient(brokerUrl, "csparql", pers);
			MqttConnectOptions opts = new MqttConnectOptions();
			opts.setCleanSession(true);
			broker.connect(opts);
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}
	
	public String getAddress(){
		return brokerUrl;
	}
	
	public void publish(String topic, String message){
		MqttMessage msg = new MqttMessage(message.getBytes());
		try {
			broker.publish(topic, msg);
		} catch (MqttPersistenceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
