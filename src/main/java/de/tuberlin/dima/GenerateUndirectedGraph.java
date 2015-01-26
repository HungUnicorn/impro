package de.tuberlin.dima;

import de.tuberlin.dima.Config;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.regex.Pattern;

// Generate undirected graph as the following logic
// If A follows B and B follows A, there is an undirected edge A-B
public class GenerateUndirectedGraph {

	public static void main(String[] args) throws Exception {

		 if (args.length == 2) {
	            getUndirectedGraph(args[0], args[1]);
	        } else{
	            System.out.println("Parameters: inputFile outputFile ");
	        }
	}
	
	public static void getUndirectedGraph(String inputPath, String outputPath) throws Exception
	{
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> input = env.readTextFile(Config
				.pathToDirectedGraph());

		/* Convert the input to edges, consisting of (source, target, 1) */
		DataSet<Tuple3<Long, Long, Integer>> arcs = input.flatMap(
				new ArcReader()).distinct();

		// Replace join by checking (source>target) then emit (Source,Target,1), otherwise emit
		// (Target, Source, 1)
		DataSet<Tuple3<Long, Long, Integer>> oneDirectionArc = arcs
				.flatMap(new OneDirectionArc());

		// Filter sum of count > 1
		ProjectOperator<Tuple3<Long, Long, Integer>, Tuple2<Long, Long>> edges = oneDirectionArc
				.groupBy(0, 1).aggregate(Aggregations.SUM, 2)
				.filter(new EdgeFilter()).project(0, 1).types(Long.class, Long.class);

		edges.print();
		edges.writeAsCsv(outputPath, "\n", " ");
		
		env.execute();
	}

	public static class ArcReader implements
			FlatMapFunction<String, Tuple3<Long, Long, Integer>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		public void flatMap(String s,
				Collector<Tuple3<Long, Long, Integer>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);

				collector.collect(new Tuple3<Long, Long, Integer>(source,
						target, (int) 1));
			}
		}
	}

	public static class OneDirectionArc
			implements
			FlatMapFunction<Tuple3<Long, Long, Integer>, Tuple3<Long, Long, Integer>> {

		public void flatMap(Tuple3<Long, Long, Integer> value,
				Collector<Tuple3<Long, Long, Integer>> collector)
				throws Exception {

			if (value.f0 < value.f1)
				collector.collect(new Tuple3<Long, Long, Integer>(value.f0,
						value.f1, value.f2));
			else
				collector.collect(new Tuple3<Long, Long, Integer>(value.f1,
						value.f0, value.f2));
		}
	}

	public static class EdgeFilter implements
			FilterFunction<Tuple3<Long, Long, Integer>> {

		public boolean filter(Tuple3<Long, Long, Integer> value)
				throws Exception {
			return value.f2 > 1;

		}
	}
}
