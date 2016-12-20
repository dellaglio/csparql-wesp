package org.streamreasoning.wsp.csparql;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;

public class RemoteRDFStreamDescriptor {
	public static void main(String[] args) {
		RemoteRDFStreamDescriptor c = new RemoteRDFStreamDescriptor("http://131.175.141.249/TripleWave-transform/sgraph");
		if(c.hasEndpoint("ws")){
			for(String uri : c.retrieveRDFStreamDescriptor("ws"))
				System.out.println("ws - " + uri);
		} else if(!c.hasEndpoint("mqtt")){
			for(String uri : c.retrieveRDFStreamDescriptor("mqtt"))
				System.out.println("mqtt - " + uri);
		}
	}
	
	private Model m;
	
	public RemoteRDFStreamDescriptor(String uri) {
		m = RDFDataMgr.loadModel(uri, Lang.JSONLD);
	}
	
	public List<String> retrieveRDFStreamDescriptor(String protocol){
		List<String> ret = new ArrayList<String>();
		Query q = QueryFactory.create(
				"SELECT ?loc "
				+ "WHERE {"
				+ "?a <http://streamreasoning.org/ontologies/SLD4TripleWave#streamLocation> ?loc"
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
				+ "?a <http://streamreasoning.org/ontologies/SLD4TripleWave#streamLocation> ?x ."
				+ "?x <http://streamreasoning.org/ontologies/SLD4TripleWave#hasProtocol> \""+protocol+"\"}");
		
		return QueryExecutionFactory.create(q, m).execAsk();
	}
}
