package org.streamreasoning.wsp.csparql;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.streamreasoning.wsp.csparql.out.mqtt.MQTTObserver;

import static org.junit.Assert.*;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.config.Config;
import eu.larkc.csparql.core.ResultFormatter;
import eu.larkc.csparql.core.engine.CsparqlEngine;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import eu.larkc.csparql.utils.Counter;
import eu.larkc.csparql.utils.TestGeneratorFromInput;

@RunWith(Parameterized.class)
public class ExternalTimestampTests {
	private CsparqlEngine engine;
	private TestGeneratorFromInput streamGenerator;

	private long[] input;
	private int width, slide;
	private List<Integer> expected;

	public ExternalTimestampTests(long[] input, int width, int slide, int[] expected){
		this.input = input;
		this.width = width;
		this.slide = slide;
		this.expected = new ArrayList<Integer>();
		for(int i : expected)
			this.expected.add(i);
	}

	/*
	 * PROBLEMS:
	 * In tumbling case, the first element is lost
	 * In sliding case, the first element of the first complete window is lost 
	 * 
	 * The contemporaneity at the edge of the windows is badly managed
	 * [6:34:46 PM] Daniele Dell'Aglio: first event at x001 is added
	 * [6:34:50 PM] Daniele Dell'Aglio: the evaluation is performed
	 * [6:35:01 PM] Daniele Dell'Aglio: then, the second event at x001 does not trigger anything
	 * [6:35:18 PM] Daniele Dell'Aglio: and then is discarded at the next (tumbling) window
	 * 
	 */

	@Parameterized.Parameters
	public static Iterable<?> data() {
		return Arrays.asList(
				new Object[][]{
					{
						new long[]{1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new int[]{2, 2}
					},{
						new long[]{600, 1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new int[]{1, 2, 2}
					},{
						new long[]{600, 1000, 1340, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new int[]{1, 3, 2}
					},{
						new long[]{600, 1000, 1340, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new int[]{1, 3, 2}
					},{
						new long[]{600, 1000, 1340, 1340, 2000, 2000, 2020, 3000, 3001}, 
						1, 1, new int[]{1, 4, 2}
					},{
						new long[]{600, 1000, 1340, 1340, 2000, 2000, 2020, 3000, 3001}, 
						2, 1, new int[]{2, 5, 6}
					},{
						new long[]{1447758448965l, 1447758449099l, 1447758449229l, 1447758449366l, 1447758449486l, 1447758449608l, 1447758449729l, 1447758449848l, 1447758449967l, 1447758450086l, 1447758450209l, 1447758450328l, 1447758450450l, 1447758450568l, 1447758450686l, 1447758450805l, 1447758450928l, 1447758451050l, 1447758451169l, 1447758451287l, 1447758451952l, 1447758452084l, 1447758452205l, 1447758452330l, 1447758452454l, 1447758452578l, 1447758452705l, 1447758452833l, 1447758452960l, 1447758453087l, 1447758453209l, 1447758453334l, 1447758453693l, 1447758453819l, 1447758453948l, 1447758454072l, 1447758454197l, 1447758454323l, 1447758454457l, 1447758454587l, 1447758454717l, 1447758454846l, 1447758454978l, 1447758455111l, 1447758455241l, 1447758455623l, 1447758455753l, 1447758455883l, 1447758456013l, 1447758456141l, 1447758456268l, 1447758456395l, 1447758456529l, 1447758456656l, 1447758456782l, 1447758456908l, 1447758457036l, 1447758457160l, 1447758457282l, 1447758457661l, 1447758457787l, 1447758457913l, 1447758458040l, 1447758458167l, 1447758458290l, 1447758458416l, 1447758458542l, 1447758458666l, 1447758458791l, 1447758458919l, 1447758459045l, 1447758459170l, 1447758459298l, 1447758459676l, 1447758459802l, 1447758459928l, 1447758460054l, 1447758460180l, 1447758460308l, 1447758460433l, 1447758460560l, 1447758460685l, 1447758460808l, 1447758460930l, 1447758461057l, 1447758461184l, 1447758461308l, 1447758461675l, 1447758461798l, 1447758461925l, 1447758462051l, 1447758462176l, 1447758462301l, 1447758462427l, 1447758462551l, 1447758462678l, 1447758462806l, 1447758462934l, 1447758463060l, 1447758463185l, 1447758463310l, 1447758463666l, 1447758463792l, 1447758463918l, 1447758464044l, 1447758464173l, 1447758464301l, 1447758464423l, 1447758464549l, 1447758464676l, 1447758464801l, 1447758464926l, 1447758465048l, 1447758465174l, 1447758465298l, 1447758465641l, 1447758465764l, 1447758465886l, 1447758466011l, 1447758466135l, 1447758466260l, 1447758466383l, 1447758466507l, 1447758466631l, 1447758466757l, 1447758466883l, 1447758467011l, 1447758467135l, 1447758467260l, 1447758467602l, 1447758467728l, 1447758467851l, 1447758467977l, 1447758468104l, 1447758468228l, 1447758468353l, 1447758468477l, 1447758468605l, 1447758468733l, 1447758468858l, 1447758468985l, 1447758469109l, 1447758469232l, 1447758469574l, 1447758469694l, 1447758469817l, 1447758469943l, 1447758470067l, 1447758470194l, 1447758470317l, 1447758470441l, 1447758470567l, 1447758470694l, 1447758470819l, 1447758470943l, 1447758471067l, 1447758471193l, 1447758471319l, 1447758471664l, 1447758471791l, 1447758471916l, 1447758472038l, 1447758472164l, 1447758472287l, 1447758472408l, 1447758472531l, 1447758472653l, 1447758472774l, 1447758472900l, 1447758473028l, 1447758473152l, 1447758473275l, 1447758473611l, 1447758473738l, 1447758473859l, 1447758473986l, 1447758474113l, 1447758474238l, 1447758474362l, 1447758474486l, 1447758474610l, 1447758474735l, 1447758474857l, 1447758474982l, 1447758475109l, 1447758475231l, 1447758475569l, 1447758475695l, 1447758475823l, 1447758475951l, 1447758476078l, 1447758476199l, 1447758476325l, 1447758476445l, 1447758476572l, 1447758476695l, 1447758476818l, 1447758476945l, 1447758477068l, 1447758477196l, 1447758477320l, 1447758477662l, 1447758477783l, 1447758477909l, 1447758478035l, 1447758478160l, 1447758478284l, 1447758478407l, 1447758478531l, 1447758478657l, 1447758478779l, 1447758478904l, 1447758479027l, 1447758479153l, 1447758479278l, 1447758479637l, 1447758479764l, 1447758479889l, 1447758480015l, 1447758480143l, 1447758480270l, 1447758480391l, 1447758480517l, 1447758480644l, 1447758480767l, 1447758480895l, 1447758481020l, 1447758481142l, 1447758481269l, 1447758481605l, 1447758481732l, 1447758481857l, 1447758481981l, 1447758482108l, 1447758482231l, 1447758482357l, 1447758482485l, 1447758482606l, 1447758482728l, 1447758482853l, 1447758482978l, 1447758483103l, 1447758483229l},
						5, 1, new int[]{1, 9, 17, 21, 28, 34, 34, 31, 35, 33, 35, 33, 36, 34, 36, 34, 36, 34, 36, 35, 37, 35, 37, 35, 36, 35, 37, 35, 37, 35, 36, 34, 36, 35, 37}
//						5, 1, new int[]{9, 17, 21, 29, 34, 34, 31, 35, 33, 35, 33, 36, 34, 36, 34, 36, 34, 36, 35, 37, 35, 37, 35, 36, 35, 37, 35, 37, 35, 36, 34, 36, 35, 37}
					},{
						new long[]{1447758448965l, 1447758449099l, 1447758449229l, 1447758449366l, 1447758449486l, 1447758449608l, 1447758449729l, 1447758449848l, 1447758449967l, 1447758450086l, 1447758450209l, 1447758450328l, 1447758450450l, 1447758450568l, 1447758450686l, 1447758450805l, 1447758450928l, 1447758451050l, 1447758451169l, 1447758451287l, 1447758451952l, 1447758452084l, 1447758452205l, 1447758452330l, 1447758452454l, 1447758452578l, 1447758452705l, 1447758452833l, 1447758452960l, 1447758453087l, 1447758453209l, 1447758453334l, 1447758453693l, 1447758453819l, 1447758453948l, 1447758454072l, 1447758454197l, 1447758454323l, 1447758454457l, 1447758454587l, 1447758454717l, 1447758454846l, 1447758454978l, 1447758455111l, 1447758455241l, 1447758455623l, 1447758455753l, 1447758455883l, 1447758456013l, 1447758456141l, 1447758456268l, 1447758456395l, 1447758456529l, 1447758456656l, 1447758456782l, 1447758456908l, 1447758457036l, 1447758457160l, 1447758457282l, 1447758457661l, 1447758457787l, 1447758457913l, 1447758458040l, 1447758458167l, 1447758458290l, 1447758458416l, 1447758458542l, 1447758458666l, 1447758458791l, 1447758458919l, 1447758459045l, 1447758459170l, 1447758459298l, 1447758459676l, 1447758459802l, 1447758459928l, 1447758460054l, 1447758460180l, 1447758460308l, 1447758460433l, 1447758460560l, 1447758460685l, 1447758460808l, 1447758460930l, 1447758461057l, 1447758461184l, 1447758461308l, 1447758461675l, 1447758461798l, 1447758461925l, 1447758462051l, 1447758462176l, 1447758462301l, 1447758462427l, 1447758462551l, 1447758462678l, 1447758462806l, 1447758462934l, 1447758463060l, 1447758463185l, 1447758463310l, 1447758463666l, 1447758463792l, 1447758463918l, 1447758464044l, 1447758464173l, 1447758464301l, 1447758464423l, 1447758464549l, 1447758464676l, 1447758464801l, 1447758464926l, 1447758465048l, 1447758465174l, 1447758465298l, 1447758465641l, 1447758465764l, 1447758465886l, 1447758466011l, 1447758466135l, 1447758466260l, 1447758466383l, 1447758466507l, 1447758466631l, 1447758466757l, 1447758466883l, 1447758467011l, 1447758467135l, 1447758467260l, 1447758467602l, 1447758467728l, 1447758467851l, 1447758467977l, 1447758468104l, 1447758468228l, 1447758468353l, 1447758468477l, 1447758468605l, 1447758468733l, 1447758468858l, 1447758468985l, 1447758469109l, 1447758469232l, 1447758469574l, 1447758469694l, 1447758469817l, 1447758469943l, 1447758470067l, 1447758470194l, 1447758470317l, 1447758470441l, 1447758470567l, 1447758470694l, 1447758470819l, 1447758470943l, 1447758471067l, 1447758471193l, 1447758471319l, 1447758471664l, 1447758471791l, 1447758471916l, 1447758472038l, 1447758472164l, 1447758472287l, 1447758472408l, 1447758472531l, 1447758472653l, 1447758472774l, 1447758472900l, 1447758473028l, 1447758473152l, 1447758473275l, 1447758473611l, 1447758473738l, 1447758473859l, 1447758473986l, 1447758474113l, 1447758474238l, 1447758474362l, 1447758474486l, 1447758474610l, 1447758474735l, 1447758474857l, 1447758474982l, 1447758475109l, 1447758475231l, 1447758475569l, 1447758475695l, 1447758475823l, 1447758475951l, 1447758476078l, 1447758476199l, 1447758476325l, 1447758476445l, 1447758476572l, 1447758476695l, 1447758476818l, 1447758476945l, 1447758477068l, 1447758477196l, 1447758477320l, 1447758477662l, 1447758477783l, 1447758477909l, 1447758478035l, 1447758478160l, 1447758478284l, 1447758478407l, 1447758478531l, 1447758478657l, 1447758478779l, 1447758478904l, 1447758479027l, 1447758479153l, 1447758479278l, 1447758479637l, 1447758479764l, 1447758479889l, 1447758480015l, 1447758480143l, 1447758480270l, 1447758480391l, 1447758480517l, 1447758480644l, 1447758480767l, 1447758480895l, 1447758481020l, 1447758481142l, 1447758481269l, 1447758481605l, 1447758481732l, 1447758481857l, 1447758481981l, 1447758482108l, 1447758482231l, 1447758482357l, 1447758482485l, 1447758482606l, 1447758482728l, 1447758482853l, 1447758482978l, 1447758483103l, 1447758483229l},
						5, 2, new int[]{9, 21, 34, 31, 33, 33, 34, 34, 34, 35, 35, 35, 35, 35, 35, 34, 35}
					},{
						new long[]{1447758448965l, 1447758449099l, 1447758449229l, 1447758449366l, 1447758449486l, 1447758449608l, 1447758449729l, 1447758449848l, 1447758449967l, 1447758450086l, 1447758450209l, 1447758450328l, 1447758450450l, 1447758450568l, 1447758450686l, 1447758450805l, 1447758450928l, 1447758451050l, 1447758451169l, 1447758451287l, 1447758451952l, 1447758452084l, 1447758452205l, 1447758452330l, 1447758452454l, 1447758452578l, 1447758452705l, 1447758452833l, 1447758452960l, 1447758453087l, 1447758453209l, 1447758453334l, 1447758453693l, 1447758453819l, 1447758453948l, 1447758454072l, 1447758454197l, 1447758454323l, 1447758454457l, 1447758454587l, 1447758454717l, 1447758454846l, 1447758454978l, 1447758455111l, 1447758455241l, 1447758455623l, 1447758455753l, 1447758455883l, 1447758456013l, 1447758456141l, 1447758456268l, 1447758456395l, 1447758456529l, 1447758456656l, 1447758456782l, 1447758456908l, 1447758457036l, 1447758457160l, 1447758457282l, 1447758457661l, 1447758457787l, 1447758457913l, 1447758458040l, 1447758458167l, 1447758458290l, 1447758458416l, 1447758458542l, 1447758458666l, 1447758458791l, 1447758458919l, 1447758459045l, 1447758459170l, 1447758459298l, 1447758459676l, 1447758459802l, 1447758459928l, 1447758460054l, 1447758460180l, 1447758460308l, 1447758460433l, 1447758460560l, 1447758460685l, 1447758460808l, 1447758460930l, 1447758461057l, 1447758461184l, 1447758461308l, 1447758461675l, 1447758461798l, 1447758461925l, 1447758462051l, 1447758462176l, 1447758462301l, 1447758462427l, 1447758462551l, 1447758462678l, 1447758462806l, 1447758462934l, 1447758463060l, 1447758463185l, 1447758463310l, 1447758463666l, 1447758463792l, 1447758463918l, 1447758464044l, 1447758464173l, 1447758464301l, 1447758464423l, 1447758464549l, 1447758464676l, 1447758464801l, 1447758464926l, 1447758465048l, 1447758465174l, 1447758465298l, 1447758465641l, 1447758465764l, 1447758465886l, 1447758466011l, 1447758466135l, 1447758466260l, 1447758466383l, 1447758466507l, 1447758466631l, 1447758466757l, 1447758466883l, 1447758467011l, 1447758467135l, 1447758467260l, 1447758467602l, 1447758467728l, 1447758467851l, 1447758467977l, 1447758468104l, 1447758468228l, 1447758468353l, 1447758468477l, 1447758468605l, 1447758468733l, 1447758468858l, 1447758468985l, 1447758469109l, 1447758469232l, 1447758469574l, 1447758469694l, 1447758469817l, 1447758469943l, 1447758470067l, 1447758470194l, 1447758470317l, 1447758470441l, 1447758470567l, 1447758470694l, 1447758470819l, 1447758470943l, 1447758471067l, 1447758471193l, 1447758471319l, 1447758471664l, 1447758471791l, 1447758471916l, 1447758472038l, 1447758472164l, 1447758472287l, 1447758472408l, 1447758472531l, 1447758472653l, 1447758472774l, 1447758472900l, 1447758473028l, 1447758473152l, 1447758473275l, 1447758473611l, 1447758473738l, 1447758473859l, 1447758473986l, 1447758474113l, 1447758474238l, 1447758474362l, 1447758474486l, 1447758474610l, 1447758474735l, 1447758474857l, 1447758474982l, 1447758475109l, 1447758475231l, 1447758475569l, 1447758475695l, 1447758475823l, 1447758475951l, 1447758476078l, 1447758476199l, 1447758476325l, 1447758476445l, 1447758476572l, 1447758476695l, 1447758476818l, 1447758476945l, 1447758477068l, 1447758477196l, 1447758477320l, 1447758477662l, 1447758477783l, 1447758477909l, 1447758478035l, 1447758478160l, 1447758478284l, 1447758478407l, 1447758478531l, 1447758478657l, 1447758478779l, 1447758478904l, 1447758479027l, 1447758479153l, 1447758479278l, 1447758479637l, 1447758479764l, 1447758479889l, 1447758480015l, 1447758480143l, 1447758480270l, 1447758480391l, 1447758480517l, 1447758480644l, 1447758480767l, 1447758480895l, 1447758481020l, 1447758481142l, 1447758481269l, 1447758481605l, 1447758481732l, 1447758481857l, 1447758481981l, 1447758482108l, 1447758482231l, 1447758482357l, 1447758482485l, 1447758482606l, 1447758482728l, 1447758482853l, 1447758482978l, 1447758483103l, 1447758483229l},
						5, 3, new int[]{17, 34, 35, 33, 36, 34, 37, 35, 37, 35, 36}
					},{
						new long[]{1447758448965l, 1447758449099l, 1447758449229l, 1447758449366l, 1447758449486l, 1447758449608l, 1447758449729l, 1447758449848l, 1447758449967l, 1447758450086l, 1447758450209l, 1447758450328l, 1447758450450l, 1447758450568l, 1447758450686l, 1447758450805l, 1447758450928l, 1447758451050l, 1447758451169l, 1447758451287l, 1447758451952l, 1447758452084l, 1447758452205l, 1447758452330l, 1447758452454l, 1447758452578l, 1447758452705l, 1447758452833l, 1447758452960l, 1447758453087l, 1447758453209l, 1447758453334l, 1447758453693l, 1447758453819l, 1447758453948l, 1447758454072l, 1447758454197l, 1447758454323l, 1447758454457l, 1447758454587l, 1447758454717l, 1447758454846l, 1447758454978l, 1447758455111l, 1447758455241l, 1447758455623l, 1447758455753l, 1447758455883l, 1447758456013l, 1447758456141l, 1447758456268l, 1447758456395l, 1447758456529l, 1447758456656l, 1447758456782l, 1447758456908l, 1447758457036l, 1447758457160l, 1447758457282l, 1447758457661l, 1447758457787l, 1447758457913l, 1447758458040l, 1447758458167l, 1447758458290l, 1447758458416l, 1447758458542l, 1447758458666l, 1447758458791l, 1447758458919l, 1447758459045l, 1447758459170l, 1447758459298l, 1447758459676l, 1447758459802l, 1447758459928l, 1447758460054l, 1447758460180l, 1447758460308l, 1447758460433l, 1447758460560l, 1447758460685l, 1447758460808l, 1447758460930l, 1447758461057l, 1447758461184l, 1447758461308l, 1447758461675l, 1447758461798l, 1447758461925l, 1447758462051l, 1447758462176l, 1447758462301l, 1447758462427l, 1447758462551l, 1447758462678l, 1447758462806l, 1447758462934l, 1447758463060l, 1447758463185l, 1447758463310l, 1447758463666l, 1447758463792l, 1447758463918l, 1447758464044l, 1447758464173l, 1447758464301l, 1447758464423l, 1447758464549l, 1447758464676l, 1447758464801l, 1447758464926l, 1447758465048l, 1447758465174l, 1447758465298l, 1447758465641l, 1447758465764l, 1447758465886l, 1447758466011l, 1447758466135l, 1447758466260l, 1447758466383l, 1447758466507l, 1447758466631l, 1447758466757l, 1447758466883l, 1447758467011l, 1447758467135l, 1447758467260l, 1447758467602l, 1447758467728l, 1447758467851l, 1447758467977l, 1447758468104l, 1447758468228l, 1447758468353l, 1447758468477l, 1447758468605l, 1447758468733l, 1447758468858l, 1447758468985l, 1447758469109l, 1447758469232l, 1447758469574l, 1447758469694l, 1447758469817l, 1447758469943l, 1447758470067l, 1447758470194l, 1447758470317l, 1447758470441l, 1447758470567l, 1447758470694l, 1447758470819l, 1447758470943l, 1447758471067l, 1447758471193l, 1447758471319l, 1447758471664l, 1447758471791l, 1447758471916l, 1447758472038l, 1447758472164l, 1447758472287l, 1447758472408l, 1447758472531l, 1447758472653l, 1447758472774l, 1447758472900l, 1447758473028l, 1447758473152l, 1447758473275l, 1447758473611l, 1447758473738l, 1447758473859l, 1447758473986l, 1447758474113l, 1447758474238l, 1447758474362l, 1447758474486l, 1447758474610l, 1447758474735l, 1447758474857l, 1447758474982l, 1447758475109l, 1447758475231l, 1447758475569l, 1447758475695l, 1447758475823l, 1447758475951l, 1447758476078l, 1447758476199l, 1447758476325l, 1447758476445l, 1447758476572l, 1447758476695l, 1447758476818l, 1447758476945l, 1447758477068l, 1447758477196l, 1447758477320l, 1447758477662l, 1447758477783l, 1447758477909l, 1447758478035l, 1447758478160l, 1447758478284l, 1447758478407l, 1447758478531l, 1447758478657l, 1447758478779l, 1447758478904l, 1447758479027l, 1447758479153l, 1447758479278l, 1447758479637l, 1447758479764l, 1447758479889l, 1447758480015l, 1447758480143l, 1447758480270l, 1447758480391l, 1447758480517l, 1447758480644l, 1447758480767l, 1447758480895l, 1447758481020l, 1447758481142l, 1447758481269l, 1447758481605l, 1447758481732l, 1447758481857l, 1447758481981l, 1447758482108l, 1447758482231l, 1447758482357l, 1447758482485l, 1447758482606l, 1447758482728l, 1447758482853l, 1447758482978l, 1447758483103l, 1447758483229l},
						5, 4, new int[]{21, 31, 33, 34, 35, 35, 35, 34}
					},{
						new long[]{1447758448965l, 1447758449099l, 1447758449229l, 1447758449366l, 1447758449486l, 1447758449608l, 1447758449729l, 1447758449848l, 1447758449967l, 1447758450086l, 1447758450209l, 1447758450328l, 1447758450450l, 1447758450568l, 1447758450686l, 1447758450805l, 1447758450928l, 1447758451050l, 1447758451169l, 1447758451287l, 1447758451952l, 1447758452084l, 1447758452205l, 1447758452330l, 1447758452454l, 1447758452578l, 1447758452705l, 1447758452833l, 1447758452960l, 1447758453087l, 1447758453209l, 1447758453334l, 1447758453693l, 1447758453819l, 1447758453948l, 1447758454072l, 1447758454197l, 1447758454323l, 1447758454457l, 1447758454587l, 1447758454717l, 1447758454846l, 1447758454978l, 1447758455111l, 1447758455241l, 1447758455623l, 1447758455753l, 1447758455883l, 1447758456013l, 1447758456141l, 1447758456268l, 1447758456395l, 1447758456529l, 1447758456656l, 1447758456782l, 1447758456908l, 1447758457036l, 1447758457160l, 1447758457282l, 1447758457661l, 1447758457787l, 1447758457913l, 1447758458040l, 1447758458167l, 1447758458290l, 1447758458416l, 1447758458542l, 1447758458666l, 1447758458791l, 1447758458919l, 1447758459045l, 1447758459170l, 1447758459298l, 1447758459676l, 1447758459802l, 1447758459928l, 1447758460054l, 1447758460180l, 1447758460308l, 1447758460433l, 1447758460560l, 1447758460685l, 1447758460808l, 1447758460930l, 1447758461057l, 1447758461184l, 1447758461308l, 1447758461675l, 1447758461798l, 1447758461925l, 1447758462051l, 1447758462176l, 1447758462301l, 1447758462427l, 1447758462551l, 1447758462678l, 1447758462806l, 1447758462934l, 1447758463060l, 1447758463185l, 1447758463310l, 1447758463666l, 1447758463792l, 1447758463918l, 1447758464044l, 1447758464173l, 1447758464301l, 1447758464423l, 1447758464549l, 1447758464676l, 1447758464801l, 1447758464926l, 1447758465048l, 1447758465174l, 1447758465298l, 1447758465641l, 1447758465764l, 1447758465886l, 1447758466011l, 1447758466135l, 1447758466260l, 1447758466383l, 1447758466507l, 1447758466631l, 1447758466757l, 1447758466883l, 1447758467011l, 1447758467135l, 1447758467260l, 1447758467602l, 1447758467728l, 1447758467851l, 1447758467977l, 1447758468104l, 1447758468228l, 1447758468353l, 1447758468477l, 1447758468605l, 1447758468733l, 1447758468858l, 1447758468985l, 1447758469109l, 1447758469232l, 1447758469574l, 1447758469694l, 1447758469817l, 1447758469943l, 1447758470067l, 1447758470194l, 1447758470317l, 1447758470441l, 1447758470567l, 1447758470694l, 1447758470819l, 1447758470943l, 1447758471067l, 1447758471193l, 1447758471319l, 1447758471664l, 1447758471791l, 1447758471916l, 1447758472038l, 1447758472164l, 1447758472287l, 1447758472408l, 1447758472531l, 1447758472653l, 1447758472774l, 1447758472900l, 1447758473028l, 1447758473152l, 1447758473275l, 1447758473611l, 1447758473738l, 1447758473859l, 1447758473986l, 1447758474113l, 1447758474238l, 1447758474362l, 1447758474486l, 1447758474610l, 1447758474735l, 1447758474857l, 1447758474982l, 1447758475109l, 1447758475231l, 1447758475569l, 1447758475695l, 1447758475823l, 1447758475951l, 1447758476078l, 1447758476199l, 1447758476325l, 1447758476445l, 1447758476572l, 1447758476695l, 1447758476818l, 1447758476945l, 1447758477068l, 1447758477196l, 1447758477320l, 1447758477662l, 1447758477783l, 1447758477909l, 1447758478035l, 1447758478160l, 1447758478284l, 1447758478407l, 1447758478531l, 1447758478657l, 1447758478779l, 1447758478904l, 1447758479027l, 1447758479153l, 1447758479278l, 1447758479637l, 1447758479764l, 1447758479889l, 1447758480015l, 1447758480143l, 1447758480270l, 1447758480391l, 1447758480517l, 1447758480644l, 1447758480767l, 1447758480895l, 1447758481020l, 1447758481142l, 1447758481269l, 1447758481605l, 1447758481732l, 1447758481857l, 1447758481981l, 1447758482108l, 1447758482231l, 1447758482357l, 1447758482485l, 1447758482606l, 1447758482728l, 1447758482853l, 1447758482978l, 1447758483103l, 1447758483229l},
						5, 5, new int[]{28, 33, 36, 35, 36, 35, 37}
//						5, 5, new int[]{29, 33, 36, 35, 36, 35, 37}
					}
				});
	}
	
	@BeforeClass public static void initialConfig(){
		Properties prop = new Properties();
		prop.put("esper.externaltime.enabled", true);
		Config.INSTANCE.setConfigParams(prop);
	}

	@Before public void setup(){
		assertEquals(true, Config.INSTANCE.isEsperUsingExternalTimestamp());
		engine = new CsparqlEngineImpl();
		engine.initialize();
		streamGenerator = new TestGeneratorFromInput("http://myexample.org/stream", input);
	}

	@After public void destroy(){
		//FIXME: concurrent exception
		//		engine.destroy();
	}

	@Test public void shouldCountSlidingWindowContents(){
		String queryGetAll = "REGISTER QUERY PIPPO AS SELECT (COUNT(*) AS ?tot) FROM STREAM <http://myexample.org/stream> [RANGE "+width+"s STEP "+slide+"s]  WHERE { ?S ?P ?O }";
		System.out.println(queryGetAll);

		engine.registerStream(streamGenerator);
		CsparqlQueryResultProxy c1 = null;

		try {
			c1 = engine.registerQuery(queryGetAll, false);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		Counter formatter = new Counter();
		c1.addObserver(formatter);
		streamGenerator.run();

		List<Integer> actual = formatter.getResults();
		//		System.out.println(actual);
		assertEquals(expected, actual);

		//		System.out.println(formatter.getResults());

	}

	//for manual checking purposes
	public static void main(String[] args) {
//		CsparqlEngine engine = new CsparqlEngineImpl();
//		engine.initialize();
//		ServiceDescriptor sd = new ServiceDescriptor(engine);
//
//		TestGeneratorFromInput streamGenerator = new TestGeneratorFromInput("http://myexample.org/stream", 
//				new long[]{1447758448965l, 1447758449099l, 1447758449229l, 1447758449366l, 1447758449486l, 1447758449608l, 1447758449729l, 1447758449848l, 1447758449967l, 1447758450086l, 1447758450209l, 1447758450328l, 1447758450450l, 1447758450568l, 1447758450686l, 1447758450805l, 1447758450928l, 1447758451050l, 1447758451169l, 1447758451287l, 1447758451952l, 1447758452084l, 1447758452205l, 1447758452330l, 1447758452454l, 1447758452578l, 1447758452705l, 1447758452833l, 1447758452960l, 1447758453087l, 1447758453209l, 1447758453334l, 1447758453693l, 1447758453819l, 1447758453948l, 1447758454072l, 1447758454197l, 1447758454323l, 1447758454457l, 1447758454587l, 1447758454717l, 1447758454846l, 1447758454978l, 1447758455111l, 1447758455241l, 1447758455623l, 1447758455753l, 1447758455883l, 1447758456013l, 1447758456141l, 1447758456268l, 1447758456395l, 1447758456529l, 1447758456656l, 1447758456782l, 1447758456908l, 1447758457036l, 1447758457160l, 1447758457282l, 1447758457661l, 1447758457787l, 1447758457913l, 1447758458040l, 1447758458167l, 1447758458290l, 1447758458416l, 1447758458542l, 1447758458666l, 1447758458791l, 1447758458919l, 1447758459045l, 1447758459170l, 1447758459298l, 1447758459676l, 1447758459802l, 1447758459928l, 1447758460054l, 1447758460180l, 1447758460308l, 1447758460433l, 1447758460560l, 1447758460685l, 1447758460808l, 1447758460930l, 1447758461057l, 1447758461184l, 1447758461308l, 1447758461675l, 1447758461798l, 1447758461925l, 1447758462051l, 1447758462176l, 1447758462301l, 1447758462427l, 1447758462551l, 1447758462678l, 1447758462806l, 1447758462934l, 1447758463060l, 1447758463185l, 1447758463310l, 1447758463666l, 1447758463792l, 1447758463918l, 1447758464044l, 1447758464173l, 1447758464301l, 1447758464423l, 1447758464549l, 1447758464676l, 1447758464801l, 1447758464926l, 1447758465048l, 1447758465174l, 1447758465298l, 1447758465641l, 1447758465764l, 1447758465886l, 1447758466011l, 1447758466135l, 1447758466260l, 1447758466383l, 1447758466507l, 1447758466631l, 1447758466757l, 1447758466883l, 1447758467011l, 1447758467135l, 1447758467260l, 1447758467602l, 1447758467728l, 1447758467851l, 1447758467977l, 1447758468104l, 1447758468228l, 1447758468353l, 1447758468477l, 1447758468605l, 1447758468733l, 1447758468858l, 1447758468985l, 1447758469109l, 1447758469232l, 1447758469574l, 1447758469694l, 1447758469817l, 1447758469943l, 1447758470067l, 1447758470194l, 1447758470317l, 1447758470441l, 1447758470567l, 1447758470694l, 1447758470819l, 1447758470943l, 1447758471067l, 1447758471193l, 1447758471319l, 1447758471664l, 1447758471791l, 1447758471916l, 1447758472038l, 1447758472164l, 1447758472287l, 1447758472408l, 1447758472531l, 1447758472653l, 1447758472774l, 1447758472900l, 1447758473028l, 1447758473152l, 1447758473275l, 1447758473611l, 1447758473738l, 1447758473859l, 1447758473986l, 1447758474113l, 1447758474238l, 1447758474362l, 1447758474486l, 1447758474610l, 1447758474735l, 1447758474857l, 1447758474982l, 1447758475109l, 1447758475231l, 1447758475569l, 1447758475695l, 1447758475823l, 1447758475951l, 1447758476078l, 1447758476199l, 1447758476325l, 1447758476445l, 1447758476572l, 1447758476695l, 1447758476818l, 1447758476945l, 1447758477068l, 1447758477196l, 1447758477320l, 1447758477662l, 1447758477783l, 1447758477909l, 1447758478035l, 1447758478160l, 1447758478284l, 1447758478407l, 1447758478531l, 1447758478657l, 1447758478779l, 1447758478904l, 1447758479027l, 1447758479153l, 1447758479278l, 1447758479637l, 1447758479764l, 1447758479889l, 1447758480015l, 1447758480143l, 1447758480270l, 1447758480391l, 1447758480517l, 1447758480644l, 1447758480767l, 1447758480895l, 1447758481020l, 1447758481142l, 1447758481269l, 1447758481605l, 1447758481732l, 1447758481857l, 1447758481981l, 1447758482108l, 1447758482231l, 1447758482357l, 1447758482485l, 1447758482606l, 1447758482728l, 1447758482853l, 1447758482978l, 1447758483103l, 1447758483229l});
//
//		String queryGetAll = 
//				"REGISTER STREAM ciao AS CONSTRUCT { ?S ?P ?O } FROM STREAM <http://myexample.org/stream> "
//						+ "[RANGE 2s STEP 1s]  "
//						+ "WHERE { ?S ?P ?O }";
//		//				"REGISTER QUERY PIPPO AS SELECT ?O FROM STREAM <http://myexample.org/stream> [RANGE 4s STEP 4s]  WHERE { ?S ?P ?O } ORDER BY ?O";
//
//		//		TestGeneratorFromFile tg = new TestGeneratorFromFile("http://myexample.org/stream", "src/test/resources/sample_input.txt");
//		engine.registerStream(streamGenerator);
//		CsparqlQueryResultProxy c1 = null;
//
//		try {
//			c1 = engine.registerQuery(queryGetAll, false);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//
//		c1.addObserver(new MQTTObserver("ciao"));
//		streamGenerator.run();

		System.out.println("A");
		CsparqlEngine engine = new CsparqlEngineRSD();
		System.out.println("B");
		engine.initialize();
		System.out.println("----------->" + engine.getAllQueries());

		TestGeneratorFromInput streamGenerator = new TestGeneratorFromInput("http://myexample.org/stream", 
				new long[]{1447758448965l, 1447758449099l, 1447758449229l, 1447758449366l, 1447758449486l, 1447758449608l, 1447758449729l, 1447758449848l, 1447758449967l, 1447758450086l, 1447758450209l, 1447758450328l, 1447758450450l, 1447758450568l, 1447758450686l, 1447758450805l, 1447758450928l, 1447758451050l, 1447758451169l, 1447758451287l, 1447758451952l, 1447758452084l, 1447758452205l, 1447758452330l, 1447758452454l, 1447758452578l, 1447758452705l, 1447758452833l, 1447758452960l, 1447758453087l, 1447758453209l, 1447758453334l, 1447758453693l, 1447758453819l, 1447758453948l, 1447758454072l, 1447758454197l, 1447758454323l, 1447758454457l, 1447758454587l, 1447758454717l, 1447758454846l, 1447758454978l, 1447758455111l, 1447758455241l, 1447758455623l, 1447758455753l, 1447758455883l, 1447758456013l, 1447758456141l, 1447758456268l, 1447758456395l, 1447758456529l, 1447758456656l, 1447758456782l, 1447758456908l, 1447758457036l, 1447758457160l, 1447758457282l, 1447758457661l, 1447758457787l, 1447758457913l, 1447758458040l, 1447758458167l, 1447758458290l, 1447758458416l, 1447758458542l, 1447758458666l, 1447758458791l, 1447758458919l, 1447758459045l, 1447758459170l, 1447758459298l, 1447758459676l, 1447758459802l, 1447758459928l, 1447758460054l, 1447758460180l, 1447758460308l, 1447758460433l, 1447758460560l, 1447758460685l, 1447758460808l, 1447758460930l, 1447758461057l, 1447758461184l, 1447758461308l, 1447758461675l, 1447758461798l, 1447758461925l, 1447758462051l, 1447758462176l, 1447758462301l, 1447758462427l, 1447758462551l, 1447758462678l, 1447758462806l, 1447758462934l, 1447758463060l, 1447758463185l, 1447758463310l, 1447758463666l, 1447758463792l, 1447758463918l, 1447758464044l, 1447758464173l, 1447758464301l, 1447758464423l, 1447758464549l, 1447758464676l, 1447758464801l, 1447758464926l, 1447758465048l, 1447758465174l, 1447758465298l, 1447758465641l, 1447758465764l, 1447758465886l, 1447758466011l, 1447758466135l, 1447758466260l, 1447758466383l, 1447758466507l, 1447758466631l, 1447758466757l, 1447758466883l, 1447758467011l, 1447758467135l, 1447758467260l, 1447758467602l, 1447758467728l, 1447758467851l, 1447758467977l, 1447758468104l, 1447758468228l, 1447758468353l, 1447758468477l, 1447758468605l, 1447758468733l, 1447758468858l, 1447758468985l, 1447758469109l, 1447758469232l, 1447758469574l, 1447758469694l, 1447758469817l, 1447758469943l, 1447758470067l, 1447758470194l, 1447758470317l, 1447758470441l, 1447758470567l, 1447758470694l, 1447758470819l, 1447758470943l, 1447758471067l, 1447758471193l, 1447758471319l, 1447758471664l, 1447758471791l, 1447758471916l, 1447758472038l, 1447758472164l, 1447758472287l, 1447758472408l, 1447758472531l, 1447758472653l, 1447758472774l, 1447758472900l, 1447758473028l, 1447758473152l, 1447758473275l, 1447758473611l, 1447758473738l, 1447758473859l, 1447758473986l, 1447758474113l, 1447758474238l, 1447758474362l, 1447758474486l, 1447758474610l, 1447758474735l, 1447758474857l, 1447758474982l, 1447758475109l, 1447758475231l, 1447758475569l, 1447758475695l, 1447758475823l, 1447758475951l, 1447758476078l, 1447758476199l, 1447758476325l, 1447758476445l, 1447758476572l, 1447758476695l, 1447758476818l, 1447758476945l, 1447758477068l, 1447758477196l, 1447758477320l, 1447758477662l, 1447758477783l, 1447758477909l, 1447758478035l, 1447758478160l, 1447758478284l, 1447758478407l, 1447758478531l, 1447758478657l, 1447758478779l, 1447758478904l, 1447758479027l, 1447758479153l, 1447758479278l, 1447758479637l, 1447758479764l, 1447758479889l, 1447758480015l, 1447758480143l, 1447758480270l, 1447758480391l, 1447758480517l, 1447758480644l, 1447758480767l, 1447758480895l, 1447758481020l, 1447758481142l, 1447758481269l, 1447758481605l, 1447758481732l, 1447758481857l, 1447758481981l, 1447758482108l, 1447758482231l, 1447758482357l, 1447758482485l, 1447758482606l, 1447758482728l, 1447758482853l, 1447758482978l, 1447758483103l, 1447758483229l});

		String queryGetAll = 
				"REGISTER STREAM ciao AS CONSTRUCT { ?S ?P ?O } FROM STREAM <http://myexample.org/stream> "
						+ "[RANGE 2s STEP 1s]  "
						+ "WHERE { ?S ?P ?O }";
		//				"REGISTER QUERY PIPPO AS SELECT ?O FROM STREAM <http://myexample.org/stream> [RANGE 4s STEP 4s]  WHERE { ?S ?P ?O } ORDER BY ?O";

		//		TestGeneratorFromFile tg = new TestGeneratorFromFile("http://myexample.org/stream", "src/test/resources/sample_input.txt");
		engine.registerStream(streamGenerator);
		CsparqlQueryResultProxy c1 = null;

		try {
			c1 = engine.registerQuery(queryGetAll, false);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		streamGenerator.run();
	}

}
