package org.streamreasoning.wsp.csparql;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.websocket.api.SuspendToken;
import org.streamreasoning.wsp.csparql.config.Config;

import eu.larkc.csparql.core.engine.CsparqlEngine;
import eu.larkc.csparql.core.streams.formats.CSparqlQuery;

public class ServiceDescriptor {
	private CsparqlEngine engine;
	private Server server;
	private HandlerCollection chc;

	public ServiceDescriptor(CsparqlEngine e){
		this.engine = e;

		server = new Server(Config.INSTANCE.getServerPort());
		
		ContextHandler mainContext = new ContextHandler("/");
		mainContext.setResourceBase(".");
		mainContext.setClassLoader(Thread.currentThread().getContextClassLoader());
		mainContext.setHandler(new AbstractHandler() {
//		server.setHandler(new AbstractHandler() {
			
			@Override
			public void handle(String target, Request baseRequest, HttpServletRequest httpRequest, HttpServletResponse httpResponse) throws IOException, ServletException {
				if(httpRequest.getPathInfo().length()==1){
					httpResponse.setContentType("application/ld+json");
					baseRequest.setHandled(true);
					httpResponse.getWriter().println("{"
							+ "\"@context\":{"
							+ "\"rsd\":\"http://example.org/RdfStreamDescriptor#\","
							+ "\"generatedAt\":{"
								+ "\"@id\":\"http://www.w3.org/ns/prov#generatedAtTime\","
								+ "\"@type\":\"http://www.w3.org/2001/XMLSchema#dateTime\""
							+ "}"
						+ "},"
						+ "\"@id\":\"someurl\","
						+ "\"@type\":\"rsd:CSPARQLEngine\","
						+ "\"sld:lastUpdated\":\"2016-11-29T16:20:10.061+0000\""
					+ "}");
				}
			}
		});
		
		ContextHandler queryContext = new ContextHandler("/stream");
		queryContext.setResourceBase(".");
		queryContext.setClassLoader(Thread.currentThread().getContextClassLoader());
		queryContext.setHandler(new AbstractHandler() {
//		server.setHandler(new AbstractHandler() {
			
			@Override
			public void handle(String target, Request baseRequest, HttpServletRequest httpRequest, HttpServletResponse httpResponse) throws IOException, ServletException {
				System.out.println(engine.getAllQueries());
				System.out.println(engine.getClass());
				String id = httpRequest.getPathInfo().substring(1);
				CSparqlQuery query = null;
				for(CSparqlQuery q : engine.getAllQueries()){
					if(q.getName().equals(id))
						query = q;
				}
				if(query!=null){
					httpResponse.setContentType("application/ld+json");
					baseRequest.setHandled(true);
					String desc = 
							"{"
								+ "\"@context\":{"
								+ "\"rsd\":\"http://example.org/RdfStreamDescriptor#\","
								+ "\"generatedAt\":{"
									+ "\"@id\":\"http://www.w3.org/ns/prov#generatedAtTime\","
									+ "\"@type\":\"http://www.w3.org/2001/XMLSchema#dateTime\""
								+ "}"
							+ "},"
							+ "\"@id\":\"someurl\","
							+ "\"@type\":\"rsd:RDFStream\","
							+ "\"dcat:distribution\":[";
					if(Config.INSTANCE.isMQTTEnabled()){
						desc += "{"
								+ "\"@id\":\""+Config.INSTANCE.getMQTTBrokerUrl()+"\","
								+ "\"rsd:protocol\":\"mqtt\","
								+ "\"dcat:accessURL\":\""+Config.INSTANCE.getMQTTBrokerUrl()+"\","
								+ "\"rsd:mqttTopic\":\""+id+"\""
							+ "}";
						if(Config.INSTANCE.isWSEnabled()) desc += ",";
					}
					if(Config.INSTANCE.isWSEnabled())
						desc += "{"
								+ "\"@id\":\""+Config.INSTANCE.getServerUrl()+"/ws/"+id+"\","
								+ "\"rsd:protocol\":\"ws\","
								+ "\"dcat:accessURL\":\""+Config.INSTANCE.getServerUrl()+"/ws/"+id+"\""
							+ "}";
					desc += "],"
							+ "\"rsd:streamTemplate\":{"
								+ "\"@id\":\"http://purl.oclc.org/NET/ssnx/ssn\""
							+ "},"
							+ "\"sld:lastUpdated\":\"2016-11-29T16:20:10.061+0000\""
						+ "}";
					httpResponse.getWriter().println(desc);
				}
			}

		});
		
//		HandlerCollection 
		chc = new HandlerCollection(true);
//		ContextHandlerCollection chc = new ContextHandlerCollection();
		chc.addHandler(mainContext);
		chc.addHandler(queryContext);
		
		server.setHandler(chc);
		
		try {
			server.start();
			System.out.println("C");
//			server.join();
			System.out.println("D");
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		System.out.println("\nRunning! Point your browsers to http://localhost:8080/ \n");
	}

	public HandlerCollection getHandlerCollection(){
		return chc;
	}
	
	public static void main(String[] args) {
	}
}
