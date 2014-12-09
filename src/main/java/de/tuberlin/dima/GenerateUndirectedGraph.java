package de.tuberlin.dima;

import de.tuberlin.dima.Config;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import java.util.regex.Pattern;

// Generate undirected graph as the following logic
// If A follows B and B follows A, there is an undirected edge A-B
public class GenerateUndirectedGraph {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> input = env.readTextFile(Config
				.pathToDirectedGraph());

		/* Convert the input to edges, consisting of (source, target) */
		DataSet<Tuple2<Long, Long>> arcs = input.flatMap(new ArcReader())
				.distinct();

		// 1->2 => 2->1
		DataSet<Tuple2<Long, Long>> reverseArcs = arcs
				.flatMap(new ArcReverse());

		FlatMapOperator<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> undirectedEdge = arcs
				.join(reverseArcs).where(0, 1).equalTo(0, 1)
				.flatMap(new UndirectedEdge());

		undirectedEdge.print();

		env.execute();
	}

	public static class ArcReader implements
			FlatMapFunction<String, Tuple2<Long, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

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

	public static class ArcReverse implements
			FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		public void flatMap(Tuple2<Long, Long> value,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			out.collect(new Tuple2<Long, Long>(value.f1, value.f0));

		}
	}

	public static class UndirectedEdge
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> edge,
				Collector<Tuple2<Long, Long>> collector) throws Exception {

			Tuple2<Long, Long> edge1 = edge.f0;
			collector.collect(edge1);
		}
	}
}
