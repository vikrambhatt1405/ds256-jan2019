package in.ds256.Assignment1.giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

public class sssp extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	
	/** The shortest paths id */
	public static final LongConfOption SOURCE_ID = new LongConfOption("SimpleShortestPathsVertex.sourceId", 1,
			"The shortest paths id");

	/** Class logger */
	private static final Logger LOG = Logger.getLogger(sssp.class);

	/**
	 * Is this vertex the source id?
	 *
	 * @param vertex Vertex
	 * @return True if the source id
	 */
	private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
		return vertex.getId().get() == SOURCE_ID.get(getConf());
	}

	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages)
			throws IOException {
		
		if (getSuperstep() == 0) {
			vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
		}
		
		double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
		for (DoubleWritable message : messages) {
			minDist = Math.min(minDist, message.get());
		}

		if (minDist < vertex.getValue().get()) {
			vertex.setValue(new DoubleWritable(minDist));
			for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
				double distance = minDist + edge.getValue().get();
				sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
			}
		}
		vertex.voteToHalt();
	}
}
