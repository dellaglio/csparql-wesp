package org.streamreasoning.wsp.csparql.in;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;

import eu.larkc.csparql.cep.api.RdfStream;

public class RemoteRDFStreamDescriptor {
	public static void main1(String[] args) {
		System.out.println(DatatypeConverter.parseDateTime("2016-12-21T15:41:54.356+01:00").getTimeInMillis());
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

	public static void main(String[] args) {
		//		RemoteRDFStreamDescriptor c = new RemoteRDFStreamDescriptor("http://131.175.141.249/TripleWave-transform/sgraph");
		String iri = "http://localhost:8114/tw";
		RemoteRDFStreamDescriptor c = new RemoteRDFStreamDescriptor(iri);
		Injecter injecter = new Injecter(new RdfStream(iri));
		if(c.hasEndpoint("ws")){
			System.out.println("== WS ==");
			for(String uri : c.retrieveRDFStreamDescriptor("ws")){
				new WebSocketConnector(uri, injecter);
			}
		} if(!c.hasEndpoint("mqtt")){
			System.out.println("== MQTT ==");
			for(String uri : c.retrieveRDFStreamDescriptor("mqtt")){
				new MQTTConnector(uri.replace("mqtt", "tcp"), "twave", null);
			}
		}
	}

}
