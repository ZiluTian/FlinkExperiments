package simulations

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java._
import org.apache.flink.graph.{Graph, Vertex, Edge}
import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, MessageIterator}
import org.apache.flink.graph.generator.GridGraph
import scala.collection.JavaConversions._

import org.apache.flink.types.LongValue
import org.apache.flink.types.NullValue
import org.apache.flink.api.common.functions.MapFunction

import scala.util.Random
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.java.io.TextInputFormat

/**
 * Implements game of life program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - write and use user-defined functions.
 */
object GameOfLife3 {
  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(args(0).toInt)
    val edgeFilePath = args(1)
    val cfreq: Int = args(2).toInt
    val interval: Int = args(3).toInt

    // Graph<K, VV, EV>    
    // Update the path to the input edge file 
    val edgeDataset: DataSet[Edge[LongValue, Int]] = env.readTextFile(edgeFilePath).map(
      new MapFunction[String, Edge[LongValue, Int]]() { 
        def map(value: String): Edge[LongValue, Int] = { 
          val fields = value.split(" ")
          val srcId: LongValue = new LongValue(fields(0).toLong)
          val targetId: LongValue = new LongValue(fields(1).toLong)
          new Edge(srcId, targetId, 0)
        }
    })

    val graph: Graph[LongValue, Array[Int], Int] = Graph.fromDataSet(edgeDataset, env).mapVertices(
      new MapFunction[Vertex[LongValue, NullValue], Array[Int]]() {
        def map(value: Vertex[LongValue, NullValue]): Array[Int] =  
          if (Random.nextBoolean()) Array(1, interval) else Array(0, interval)
        }
      )

    // define the maximum number of iterations
    val maxIterations = 200

    final class GoLComputeFunction extends ComputeFunction[LongValue, Array[Int], Int, Array[Int]] {

        override def compute(vertex: Vertex[LongValue, Array[Int]], messages: MessageIterator[Array[Int]]): Unit = {            
            var aliveNeighbors: Int = 0
            val states: Array[Int] = vertex.getValue().asInstanceOf[Array[Int]]
            var alive: Int = states(0)
            var idleCountDown: Int = states(1)

            if (vertex.getId().getValue != 0) { // cell
                if (idleCountDown > 1) {    // comp. interval
                    idleCountDown -= 1
                } else {    // communication frequency
                    while (messages.hasNext) {
                        val msg = messages.next.head
                        aliveNeighbors += msg
                    }
                    if ((alive == 1) && ((aliveNeighbors > 3) || (aliveNeighbors < 2))) {
                        alive = 0
                    } else if ((alive == 0) && (aliveNeighbors == 3)){
                        alive = 1
                    } 
                    setNewVertexValue(Array(alive, idleCountDown))
                    val it = getEdges.iterator()
                    while (it.hasNext) {
                        val edge = it.next
                        Range(0, cfreq).foreach(i => 
                          sendMessageTo(edge.getTarget, Array(alive)))
                    }
                }
            } else { // clock
                val it = getEdges.iterator()
                while (it.hasNext) {
                    val edge = it.next
                    sendMessageTo(edge.getTarget, Array(0))
                }
            }
      }
    }

    // message combiner
    // combinedMessage has the same type as Message
    final class GoLCombiner extends MessageCombiner[LongValue, Array[Int]] {

        override def combineMessages(messages: MessageIterator[Array[Int]]): Unit = {

            var combined: Array[Int] = Array()

            while (messages.hasNext) {
              val msg = messages.next
              combined = msg ++ combined
            }
            sendCombinedMessage(combined)
        }
    }

    measure.apply(() => {
      // Execute the vertex-centric iteration
      val result = graph.runVertexCentricIteration(new GoLComputeFunction(), new GoLCombiner(), maxIterations)
      // Extract the vertices as the result
      val golResult = result.getVertices
      // Execute
      golResult.collect()
    }, maxIterations)
  }
}
