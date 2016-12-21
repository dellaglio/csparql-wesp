package org.streamreasoning.wsp.csparql;

import java.io.IOException;

import org.streamreasoning.wsp.csparql.out.mqtt.MQTTBroker;

import eu.larkc.csparql.core.engine.CsparqlEngine;
import eu.larkc.csparql.core.streams.formats.CSparqlQuery;
import fi.iki.elonen.NanoHTTPD;

import fi.iki.elonen.NanoHTTPD.Response.Status;

public class ServiceDescriptor extends NanoHTTPD {
	private CsparqlEngine engine;

	public ServiceDescriptor(CsparqlEngine engine){
		super(8080);
		this.engine = engine;
		try {
			start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("\nRunning! Point your browsers to http://localhost:8080/ \n");
	}

	public static void main(String[] args) {
		new ServiceDescriptor(null);
	}

	@Override
	public Response serve(IHTTPSession session) {
		String uri = session.getUri();
		System.out.println(uri);
		Response ret;
		if(uri.equals("/")){
			String msg = 
					"{"
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
					+ "}";
			ret = newFixedLengthResponse(msg);
			ret.setMimeType("application/ld+json");
			return ret;
		}
		if(uri.startsWith("/stream/")){
			String id = uri.substring("/stream/".length());
			CSparqlQuery query = null;
			for(CSparqlQuery q : engine.getAllQueries()){
				if(q.getName().equals(id))
					query = q;
			}
			if(query!=null){
				String msg = 
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
							+ "\"dcat:distribution\":{"
								+ "\"@id\":\""+MQTTBroker.INSTANCE.getAddress()+"\","
								+ "\"rsd:protocol\":\"mqtt\","
								+ "\"dcat:accessURL\":\""+MQTTBroker.INSTANCE.getAddress()+"\","
								+ "\"rsd:mqttTopic\":\""+id+"\""
							+ "},"
							+ "\"rsd:streamTemplate\":{"
								+ "\"@id\":\"http://purl.oclc.org/NET/ssnx/ssn\""
							+ "},"
							+ "\"sld:lastUpdated\":\"2016-11-29T16:20:10.061+0000\""
						+ "}";
				ret = newFixedLengthResponse(msg);
				ret.setMimeType("application/ld+json");
				return ret;
			}
		}
		return newFixedLengthResponse(Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "Error 404, file not found.");
	}
}
