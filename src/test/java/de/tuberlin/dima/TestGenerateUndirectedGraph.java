package de.tuberlin.dima;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;



public class TestGenerateUndirectedGraph {
	
		Set<String> testResultSet;

	    Set<String> knownAnswerSet;

	    String outputDirectoryPath;

	    File outputDirectory;

	    String testInputPath;

	    File testAnswerFile;

	    
	    public void setUp() throws Exception {
	        testResultSet = new HashSet<String>();
	        knownAnswerSet = new HashSet<String>();

	        testInputPath = String.format("/home/amit/impro/test1");
	        testAnswerFile = new File("/home/amit/impro/ansTest1");
	        outputDirectoryPath = String.format("/home/amit/impro/output1");
	        outputDirectory = new File(outputDirectoryPath);
  
	        

	        GenerateUndirectedGraph.getUndirectedGraph(testInputPath, outputDirectoryPath);

	        String line;
	        for (final File fileEntry : outputDirectory.listFiles()) {
	            BufferedReader br = new BufferedReader(new FileReader(fileEntry));
	            while ((line = br.readLine()) != null) {
	                testResultSet.add(line);
	            }
	            br.close();
	        }

	        BufferedReader br = new BufferedReader(new FileReader(testAnswerFile));
	        while ((line = br.readLine()) != null) {
	            knownAnswerSet.add(line);
	        }
	        br.close();

	    }

	    @Test
	    public void test() throws Exception {
	    	setUp();
	    	junit.framework.TestCase.assertTrue(knownAnswerSet.containsAll(testResultSet));
	    	junit.framework.TestCase.assertTrue(testResultSet.containsAll(knownAnswerSet));
	    }

}
