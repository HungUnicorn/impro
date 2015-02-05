package de.tuberlin.dima;

import java.util.regex.Pattern;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Get the highest degree from undirected graph
public class HighestDegree {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> input = env.readTextFile(Config
				.pathToUndirectedGraph());

		/* Convert the input to edges, consisting of (source, target) */
		DataSet<Tuple2<Long, Long>> edges = input.flatMap(new EdgeReader())
				.distinct();

		/* Compute the outdegree of every vertex, though it's undirected graph */
		DataSet<Tuple2<Long, Long>> verticesWithOutDegree = edges.project(0)
				.types(Long.class).groupBy(0).reduceGroup(new DegreeOfVertex());

		DataSet<Tuple2<Long, Long>> verticesWithInDegree = edges.project(1)
				.types(Long.class).groupBy(0).reduceGroup(new DegreeOfVertex());

		// Degree = Union outDegree with indegree
		DataSet<Tuple2<Long, Long>> vericesWithDegree = verticesWithOutDegree
				.union(verticesWithInDegree).groupBy(0).sum(1);

		// Output(1, node, degree)
		DataSet<Tuple3<Long, Long, Long>> topKMapper = vericesWithDegree
				.flatMap(new TopKMapper());
		
		// Output(Maxdegree), sort on degree
		DataSet<Tuple1<Long>> topKReducer = topKMapper.groupBy(0)
				.sortGroup(2, Order.DESCENDING).first(1).project(2).types(Long.class);

		topKReducer.writeAsCsv(Config.maxDegree(), WriteMode.OVERWRITE);
		
		env.execute();
	}

	public static class DegreeOfVertex implements
			GroupReduceFunction<Tuple1<Long>, Tuple2<Long, Long>> {

		@Override
		public void reduce(Iterable<Tuple1<Long>> tuples,
				Collector<Tuple2<Long, Long>> collector) throws Exception {

			Iterator<Tuple1<Long>> iterator = tuples.iterator();
			Long vertexId = iterator.next().f0;

			long count = 1L;
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}
			collector.collect(new Tuple2<Long, Long>(vertexId, count));
		}
	}

	public static class EdgeReader implements
			FlatMapFunction<String, Tuple2<Long, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<Long, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);

				collector.collect(new Tuple2<Long, Long>(source, target));
			}
		}
	}

	public static class TopKMapper implements
			FlatMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>> {

		private TreeMap<Long, Long> recordMap = new TreeMap<Long, Long>(
				Collections.reverseOrder());

		@Override
		public void flatMap(Tuple2<Long, Long> tuple,
				Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
			collector.collect(new Tuple3<Long, Long, Long>((long) 1, tuple.f0,
					tuple.f1));
		}
	}

}
