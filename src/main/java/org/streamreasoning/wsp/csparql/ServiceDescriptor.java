package org.streamreasoning.wsp.csparql;

import java.io.IOException;

import org.streamreasoning.wsp.csparql.mqtt.MQTTBroker;

import eu.larkc.csparql.core.engine.CsparqlEngine;
import eu.larkc.csparql.core.streams.formats.CSparqlQuery;
import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoHTTPD.Response.IStatus;
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
		//		ret.setConHeader("Content-Type", "application/json");
		//		ret.addHeader("Link", "<http://json-ld.org/contexts/person.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
		String uri = session.getUri();
		System.out.println(uri);
		Response ret;
		if(uri.startsWith("/stream/")){
			String id = uri.substring("/stream/".length());
			CSparqlQuery query = null;
			for(CSparqlQuery q : engine.getAllQueries()){
				if(q.getName().equals(id))
					query = q;
			}
			if(query!=null){
				String msg = "{\"@context\":{\"sld\":\"http://streamreasoning.org/ontologies/SLD4TripleWave#\",\"generatedAt\":{\"@id\":\"http://www.w3.org/ns/prov#generatedAtTime\",\"@type\":\"http://www.w3.org/2001/XMLSchema#dateTime\"}},\"@id\":\"tr:sGraph\",\"sld:streamLocation\":\""+MQTTBroker.INSTANCE.getAddress()+"\",\"sld:streamTopic\":\""+id+"\",\"sld:tBoxLocation\":{\"@id\":\"http://purl.oclc.org/NET/ssnx/ssn\"},\"sld:lastUpdated\":\"2016-11-29T16:20:10.061+0000\"}";
				ret = newFixedLengthResponse(msg);
				//		Response ret = new NanoHTTPD.Response(Response.Status.OK, "application/json", );
				ret.setMimeType("application/json");
				return ret;
			}
		}
		return newFixedLengthResponse(Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "Error 404, file not found.");
	}
}
