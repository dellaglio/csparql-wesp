package org.streamreasoning.wsp.csparql.out.mqtt;

import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Observable;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.mem.GraphMem;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.core.ResultFormatter;

public class MQTTObserver extends ResultFormatter{
	private String topic;
	private boolean first = true;
	
	public MQTTObserver(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void update(Observable o, Object arg) {
		if(arg instanceof RDFTable){
			Iterator<RDFTuple> it = ((RDFTable)arg).iterator();
			Graph graph = new GraphMem();
			while(it.hasNext()){
				RDFTuple t = it.next();
				graph.add(Triple.create(
						NodeFactory.createURI(t.get(0)), 
						NodeFactory.createURI(t.get(1)), 
						t.get(2).startsWith("http") ? NodeFactory.createURI(t.get(2)) : NodeFactory.createLiteral(t.get(2))));
			}
			Dataset ds = DatasetFactory.createMem();
			long ts = System.currentTimeMillis();
			String graphName = "http://example.org/"+topic+"/"+ts;
			ds.addNamedModel(graphName, ModelFactory.createModelForGraph(graph));
			Model def = ModelFactory.createDefaultModel();
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			String date = sdf.format(new Date(ts));
			System.out.println(date);
			
			def.add(ResourceFactory.createResource(graphName),
					ResourceFactory.createProperty("http://www.w3.org/ns/prov#generatedAt"),
//					ResourceFactory.createTypedLiteral(date, XSDDateTimeType.XSDdateTime));
					ResourceFactory.createPlainLiteral(date));
			ds.setDefaultModel(def);
			
			StringWriter out = new StringWriter();
			RDFDataMgr.write(out, ds, RDFFormat.JSONLD_FLAT);
//			System.out.println(out.toString());
			if(first){
				MQTTBroker.INSTANCE.publish(topic, "["+out.toString());
				first = false;
			} else
				MQTTBroker.INSTANCE.publish(topic, ","+out.toString());
		} else {throw new RuntimeException();}
	}

}
