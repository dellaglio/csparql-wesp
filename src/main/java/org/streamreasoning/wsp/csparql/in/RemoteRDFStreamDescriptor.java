package org.streamreasoning.wsp.csparql.in;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.core.RDFDataset.Quad;
import com.github.jsonldjava.utils.JsonUtils;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;

public class RemoteRDFStreamDescriptor {
	public static void main1(String[] args) {
		System.out.println(DatatypeConverter.parseDateTime("2016-12-21T15:41:54.356+01:00").getTimeInMillis());
	}
	
	public static void main(String[] args) {
//		RemoteRDFStreamDescriptor c = new RemoteRDFStreamDescriptor("http://131.175.141.249/TripleWave-transform/sgraph");
		RemoteRDFStreamDescriptor c = new RemoteRDFStreamDescriptor("http://localhost:8114");
		if(c.hasEndpoint("ws")){
			System.out.println("== WS ==");
			for(String uri : c.retrieveRDFStreamDescriptor("ws"))
				System.out.println("- " + uri);
		} if(c.hasEndpoint("mqtt")){
			System.out.println("== MQTT ==");
			for(String uri : c.retrieveRDFStreamDescriptor("mqtt")){
				System.out.println("- " + uri);
				try {
					MqttClient client = new MqttClient(uri.replace("mqtt", "tcp"), "client");
					client.connect();
					client.setCallback(new MqttCallback() {
						
						@Override
						public void messageArrived(String topic, MqttMessage message) throws Exception {
							System.out.println(message);
							RDFDataset ds = (RDFDataset) JsonLdProcessor.toRDF(JsonUtils.fromString(message.toString()));
							String graphName = null,
									timestamp = null;
							for(Quad q : ds.getQuads("@default"))
								if(q.getPredicate().getValue().equals("http://www.w3.org/ns/prov#generatedAtTime")){
									graphName = q.getSubject().getValue();
									timestamp = q.getObject().getValue();
								}
							if(graphName!=null){
								for(Quad q : ds.getQuads(graphName))
									System.out.println(q.getSubject() + " " + q.getPredicate() + " " + q.getObject() + " " + DatatypeConverter.parseDateTime(timestamp).getTimeInMillis());
							}
						}
						
						@Override
						public void deliveryComplete(IMqttDeliveryToken token) {
							// TODO Auto-generated method stub
							
						}
						
						@Override
						public void connectionLost(Throwable cause) {
							System.err.println("Connection lost");
							cause.printStackTrace();
						}
					});
					client.subscribe("twave");
				} catch (MqttException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	private Model m;
	
	public RemoteRDFStreamDescriptor(String uri) {
		m = RDFDataMgr.loadModel(uri, Lang.JSONLD);
		System.out.println(m.size());
	}
	
	public List<String> retrieveRDFStreamDescriptor(String protocol){
		List<String> ret = new ArrayList<String>();
		Query q = QueryFactory.create(
				"SELECT ?loc "
				+ "WHERE {"
				+ "?a <http://www.w3.org/ns/dcat#distribution> ?d . "
				+ "?d <http://something/RdfStreamDescription#protocol> \""+protocol+"\" ; "
				+ "<http://www.w3.org/ns/dcat#accessURL> ?loc"
				+ "}");
		
		ResultSet rs = QueryExecutionFactory.create(q, m).execSelect();
		while(rs.hasNext()){
			ret.add(rs.nextSolution().get("loc").toString());
		}
		return ret;
	}
	
	public boolean hasEndpoint(String protocol){
		Query q = QueryFactory.create(
				"ASK "
				+ "WHERE {"
				+ "?a <http://www.w3.org/ns/dcat#distribution> ?d . "
				+ "?d <http://something/RdfStreamDescription#protocol> \""+protocol+"\""
				+ "}");
		
		return QueryExecutionFactory.create(q, m).execAsk();
	}
}
