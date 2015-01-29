package de.tuberlin.dima;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.junit.Test;

/* the colors used should be at most max(degree) + 1
 * See bound: http://en.wikipedia.org/wiki/Graph_coloring#Bounds_on_the_chromatic_number*/
public class TestGraphColoring extends TestCase {

	Set<String> testResultSet;

	private int maxDegree;
	private int colors;

	String outputDirectoryPath;

	File outputDirectory;

	String testInputPath;

	File testAnswerFile;

	public void setUp() throws Exception {
		testResultSet = new HashSet<String>();

		testAnswerFile = new File(Config.maxDegree());		
		outputDirectory = new File(Config.graphColoring());
		
		String s;
		Pattern SEPARATOR = Pattern.compile("[ \t,]");

		for (final File fileEntry : outputDirectory.listFiles()) {
			BufferedReader br = new BufferedReader(new FileReader(fileEntry));
			while ((s = br.readLine()) != null) {
				String[] tokens = SEPARATOR.split(s);
				testResultSet.add(tokens[1]);
			}
			br.close();
		}

		BufferedReader br = new BufferedReader(new FileReader(testAnswerFile));
		while ((s = br.readLine()) != null) {
			maxDegree = Integer.parseInt(s);
		}
		br.close();

	}

	@Test
	public void testColorsUsedBound() throws Exception {
		setUp();
		colors = testResultSet.size();
		assertTrue("Colors (" + colors + ") should be at most max degree + 1 ("
				+ (maxDegree +1) + ")", colors <= maxDegree + 1);

	}
}
