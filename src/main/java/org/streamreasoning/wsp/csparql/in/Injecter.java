package org.streamreasoning.wsp.csparql.in;

import java.io.IOException;

import javax.xml.bind.DatatypeConverter;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.core.RDFDataset.Quad;
import com.github.jsonldjava.utils.JsonUtils;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;

public class Injecter {
	RdfStream stream;

	public Injecter(RdfStream stream) {
		this.stream = stream;
	}
	
	public void pushDataItem(String message){
		try {
			RDFDataset ds;
			ds = (RDFDataset) JsonLdProcessor.toRDF(JsonUtils.fromString(message));
			String graphName = null,
					timestamp = null;
			for(Quad q : ds.getQuads("@default"))
				if(q.getPredicate().getValue().equals("http://www.w3.org/ns/prov#generatedAtTime")){
					graphName = q.getSubject().getValue();
					timestamp = q.getObject().getValue();
				}
			if(graphName!=null){
				for(Quad q : ds.getQuads(graphName)){
					System.out.println(q.getSubject() + " " + q.getPredicate() + " " + q.getObject() + " " + DatatypeConverter.parseDateTime(timestamp).getTimeInMillis());
					RdfQuadruple dataItem = new RdfQuadruple(q.getSubject().getValue(), q.getPredicate().getValue(), q.getObject().getValue(), DatatypeConverter.parseDateTime(timestamp).getTimeInMillis());
					stream.put(dataItem);
				}
			}
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonLdError e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
