package in.ds256.Assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class maxValue {

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setAppName("Max Value Propogation");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputRDD = sc.textFile(inputFile);
		Boolean hasConverged = false;
		
		JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> graphRDD = inputRDD.mapToPair(adjacencyList -> { 
			String[] tokens = adjacencyList.split(" ");
			ArrayList<Long> neighbours = new ArrayList<Long>();
			
			//FIXME: What would happen if the input read is empty ?
			if(tokens.length>1) {
				for( int i=1; i<tokens.length; i++) {
					neighbours.add(Long.parseLong(tokens[i]));
				}
			} 
			
			// SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isActive, vertexState>>
			return new Tuple2<Long, Tuple3<ArrayList<Long>, Boolean, Long>>(Long.parseLong(tokens[0]), new Tuple3<ArrayList<Long>, Boolean, Long>(neighbours, true, Long.parseLong(tokens[0])));
		});
		
		// Control will exit this loop when the algorithm has converged.
		while(!hasConverged) {
			JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> messages = graphRDD.flatMapToPair(vertex -> { 
				ArrayList<Tuple2<Long, Tuple3<ArrayList<Long>, Boolean, Long>>> tuples = new ArrayList<Tuple2<Long, Tuple3<ArrayList<Long>, Boolean, Long>>>();
				ArrayList<Long> neighbours = vertex._2._1();
				
				for(Long neighbour : neighbours) {
					//FIXME: Here, it really does not matter what values you pass for _1 and _2 of Tuple3. But can you optimize the execution if you make intelligent use of it ?
					tuples.add(new Tuple2<Long, Tuple3<ArrayList<Long>,Boolean,Long>>(neighbour, new Tuple3<>(null, true, vertex._1)));
				}
				return  tuples.iterator();
			});
			
			
			graphRDD = graphRDD
					.union(messages)
					.groupByKey()
					.mapToPair(vertex -> {
						
						//FIXME: This piece of code will fail to converge in a specific case. 
						//FIXME: The said case is not so uncommon in the real world and you guys have to think what it is and how it can be fixed !
						
						Long vertexID=vertex._1, vertexState = Long.MAX_VALUE, maxVertexState=Long.MIN_VALUE;
						ArrayList<Long> neighbours = null;
						
						Iterator<Tuple3<ArrayList<Long>, Boolean, Long>> iter = vertex._2.iterator();
						while(iter.hasNext()) {
							Tuple3<ArrayList<Long>, Boolean, Long> message = iter.next();
							if(!message._1().isEmpty()) {
								//You have found the neghbourList and the original state !
								neighbours=message._1();
								vertexState=message._3();
							}
							
							if(message._3()>maxVertexState) {
								maxVertexState=message._3();
							}
						}
						
						if(!neighbours.isEmpty()) {
							if(vertexState<maxVertexState) {
								return new Tuple2<Long, Tuple3<ArrayList<Long>, Boolean, Long>>( vertexID, new Tuple3<ArrayList<Long>, Boolean, Long>(neighbours, true, maxVertexState));
							} else {
								return new Tuple2<Long, Tuple3<ArrayList<Long>, Boolean, Long>>( vertexID, new Tuple3<ArrayList<Long>, Boolean, Long>(neighbours, false, vertexState));
							}
						} else {
							return new Tuple2<Long, Tuple3<ArrayList<Long>, Boolean, Long>>( vertexID, new Tuple3<ArrayList<Long>, Boolean, Long>(neighbours, true, maxVertexState));
						}
					});
			
			//Looking for at-least one active vertex ?
			//FIXME : This check can be further optimized; You guys should think about it !
			hasConverged = graphRDD.filter(vertex -> vertex._2._2()).count() > 0 ? false : true;
		}
		
		// store's computed vertexState to HDFS
		graphRDD.mapToPair(vertex -> { 
			return new Tuple2<Long, Long>(vertex._1, vertex._2._3());
		}).saveAsTextFile(outputFile);
		
		sc.stop();
		sc.close();
		
	}
}
