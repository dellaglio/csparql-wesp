package org.streamreasoning.wsp.csparql;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.streamreasoning.wsp.csparql.config.Config;
import org.streamreasoning.wsp.csparql.out.mqtt.MQTTObserver;
import org.streamreasoning.wsp.csparql.out.ws.WSQueryCreator;
import org.streamreasoning.wsp.csparql.out.ws.WSQueryObserver;

import eu.larkc.csparql.common.utils.ReasonerChainingType;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;

public class CsparqlEngineRSD extends CsparqlEngineImpl{
	private Map<String,WSQueryObserver> wsObs;
	private ServiceDescriptor serviceDescriptor;
	
	public CsparqlEngineRSD() {
		super();
		serviceDescriptor = new ServiceDescriptor(this);
		wsObs = new HashMap<String,WSQueryObserver>();
	}
	
	@Override
	public CsparqlQueryResultProxy registerQuery(String command, boolean activateInference) throws ParseException {
		CsparqlQueryResultProxy c = super.registerQuery(command, activateInference);
		//MQTT
		if(Config.INSTANCE.isMQTTEnabled())
			c.addObserver(new MQTTObserver(c.getName()));
		//WS
		if(Config.INSTANCE.isWSEnabled()){
			WSQueryObserver obs = new WSQueryObserver();
			c.addObserver(obs);
			wsObs.put(c.getName(), obs);
			
			ContextHandler chWs = new ContextHandler();
			chWs.setContextPath("/ws/"+c.getName());
			
			chWs.setHandler(new WebSocketHandler() {
				@Override
				public void configure(WebSocketServletFactory factory) {
					factory.setCreator(new WSQueryCreator(obs));
				}
			});
			serviceDescriptor.getHandlerCollection().addHandler(chWs);
		}
		return c;
	}
	
	@Override
	public CsparqlQueryResultProxy registerQuery(String command, boolean activateInference,
			String rulesFileSerialization, ReasonerChainingType chainingType, String tBoxFileSerialization)
			throws ParseException {
		CsparqlQueryResultProxy c = super.registerQuery(command, activateInference, rulesFileSerialization, chainingType, tBoxFileSerialization);
		//MQTT
		if(Config.INSTANCE.isMQTTEnabled())
			c.addObserver(new MQTTObserver(c.getName()));
		//WS
		if(Config.INSTANCE.isWSEnabled()){
			WSQueryObserver obs = new WSQueryObserver();
			c.addObserver(obs);
			wsObs.put(c.getName(), obs);
			
			ContextHandler chWs = new ContextHandler();
			chWs.setContextPath("/ws/"+c.getName());
			
			chWs.setHandler(new WebSocketHandler() {
				@Override
				public void configure(WebSocketServletFactory factory) {
					factory.setCreator(new WSQueryCreator(obs));
				}
			});
			serviceDescriptor.getHandlerCollection().addHandler(chWs);
		}
		return c;
	}
	
	@Override
	public CsparqlQueryResultProxy registerQuery(String command, boolean activateInference,
			String tBoxFileSerialization) throws ParseException {
		CsparqlQueryResultProxy c = super.registerQuery(command, activateInference, tBoxFileSerialization);
		//MQTT
		if(Config.INSTANCE.isMQTTEnabled())
			c.addObserver(new MQTTObserver(c.getName()));
		//WS
		if(Config.INSTANCE.isWSEnabled()){
			WSQueryObserver obs = new WSQueryObserver();
			c.addObserver(obs);
			wsObs.put(c.getName(), obs);
			
			ContextHandler chWs = new ContextHandler();
			chWs.setContextPath("/ws/"+c.getName());
			
			chWs.setHandler(new WebSocketHandler() {
				@Override
				public void configure(WebSocketServletFactory factory) {
					factory.setCreator(new WSQueryCreator(obs));
				}
			});
			serviceDescriptor.getHandlerCollection().addHandler(chWs);
		}
		return c;
	}
	
	@Override
	public CsparqlQueryResultProxy registerQuery(String command, boolean activateInference,
			String rulesFileSerialization, ReasonerChainingType chainingType) throws ParseException {
		CsparqlQueryResultProxy c = super.registerQuery(command, activateInference, rulesFileSerialization, chainingType);
		//MQTT
		if(Config.INSTANCE.isMQTTEnabled())
			c.addObserver(new MQTTObserver(c.getName()));
		//WS
		if(Config.INSTANCE.isWSEnabled()){
			WSQueryObserver obs = new WSQueryObserver();
			c.addObserver(obs);
			wsObs.put(c.getName(), obs);
			
			ContextHandler chWs = new ContextHandler();
			chWs.setContextPath("/ws/"+c.getName());
			
			chWs.setHandler(new WebSocketHandler() {
				@Override
				public void configure(WebSocketServletFactory factory) {
					factory.setCreator(new WSQueryCreator(obs));
				}
			});
			serviceDescriptor.getHandlerCollection().addHandler(chWs);
		}
		return c;
	}
	
	
}
