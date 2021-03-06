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
 * See upper bound: http://en.wikipedia.org/wiki/Graph_coloring#Bounds_on_the_chromatic_number*/
public class TestGraphColoring extends TestCase {

	Set<String> testResultSet;
	private int degree;

	private int maxDegree = 0;
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

		for (final File fileEntry : testAnswerFile.listFiles()) {
			BufferedReader br = new BufferedReader(new FileReader(fileEntry));
			while ((s = br.readLine()) != null) {
				degree = Integer.parseInt(s);

				if (degree > maxDegree)
					maxDegree = degree;
			}
			br.close();
		}

	}

	@Test
	public void testColorsUpperBound() throws Exception {
		setUp();
		colors = testResultSet.size();
		assertTrue("Colors (" + colors + ") should be at most max degree + 1 ("
				+ (maxDegree + 1) + ")", colors <= maxDegree + 1);

	}
}
