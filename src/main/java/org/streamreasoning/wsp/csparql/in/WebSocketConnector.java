package org.streamreasoning.wsp.csparql.in;

import java.net.URI;

import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

public class WebSocketConnector {
	public WebSocketConnector(String uri, Injecter injecter){
		WebSocketClient client = new WebSocketClient();
		try {
			client.start();
			WebSocketListener ses = new WebSocketListener(injecter);
			ClientUpgradeRequest cur = new ClientUpgradeRequest();
			client.connect(ses, new URI(uri), cur);
			System.out.println("Connecting to "+uri);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}

