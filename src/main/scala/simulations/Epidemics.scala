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
import scala.math.{min, max}
import breeze.stats.distributions.Gamma

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
object Epidemics {
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
        def map(value: Vertex[LongValue, NullValue]): Array[Int] =  {
          val age: Int = Random.nextInt(90) + 10
          val symptomatic: Int = if (Random.nextBoolean) 1 else 0
          val health: Int = if (Random.nextInt(100)==0) 2 else 0
          val vulnerability: Int = if (age > 60) 1 else 0
          val daysInfected: Int = 0
          Array(age, symptomatic, health, vulnerability, daysInfected, interval)
        }
      }
    )


    // define the maximum number of iterations
    val maxIterations = 50

    // Encodings (health states)
    val Susceptible: Int = 0
    val Exposed: Int = 1
    val Infectious: Int = 2
    val Hospitalized: Int = 3
    val Recover: Int = 4
    val Deceased: Int = 5
    // Encodings (vulnerability levels)
    val Low: Int = 0
    val High: Int = 1
    // Infectious parameter (gamma distribution)
    val infectiousAlpha = 0.25
    val infectiousBeta = 1
    val symptomaticSkew = 2

    def change(health: Int, vulnerability: Int): Int = {
        health match {
            case Susceptible => Exposed
            case Exposed => 
                val worse_prob: Double = eval(vulnerability, Exposed, Infectious)
                if (Random.nextDouble < worse_prob) {
                    Infectious
                } else {
                    Recover
                }
            case Infectious => 
                val worse_prob: Double = eval(vulnerability, Infectious, Hospitalized)
                if (Random.nextDouble < worse_prob) {
                    Hospitalized
                } else {
                    Recover
                }
            case Hospitalized =>
                val worse_prob: Double = eval(vulnerability, Hospitalized, Deceased)
                if (Random.nextDouble < worse_prob) {
                    Deceased
                } else {
                    Recover
                }
            case _ => health
        }
    }

    def stateDuration(health: Int): Int = {
        val randDuration: Int = (3*Random.nextGaussian()).toInt

        health match {
            case Infectious => max(2, randDuration+6) 
            case Hospitalized => max(2, randDuration+7) 
            case Exposed => max(3, randDuration+5)
        }
    }

    def infectiousness(health: Int, symptomatic: Boolean): Double = {
        if (health == Infectious) {
            var gd = Gamma(infectiousAlpha, infectiousBeta).draw()
            if (symptomatic){
                gd = gd * 2
            }
            gd
        } else {
            0
        }
    }

    def eval(vulnerability: Int, currentHealth: Int, projectedHealth: Int): Double = {
        vulnerability match {
            case Low =>
                (currentHealth, projectedHealth) match {
                    case (Exposed, Infectious) => 0.6
                    case (Infectious, Hospitalized) => 0.1
                    case (Hospitalized, Deceased) => 0.1
                    case _ => 0.01
                }
            case High =>
                (currentHealth, projectedHealth) match {
                    case (Exposed, Infectious) => 0.9
                    case (Infectious, Hospitalized) => 0.4
                    case (Hospitalized, Deceased) => 0.5
                    case _ => 0.05
                }
        }
    }


    final class EpidemicsComputeFunction extends ComputeFunction[LongValue, Array[Int], Int, Array[Double]] {

        override def compute(vertex: Vertex[LongValue, Array[Int]], messages: MessageIterator[Array[Double]]): Unit = {
          val state = vertex.getValue
          val age: Int = state(0)
          val symptomatic: Int = state(1)
          var health: Int = state(2)
          val vulnerability: Int = state(3)
          var daysInfected: Int = state(4)
          var idleCountDown: Int = state(5)

          if (vertex.getId().getValue != 0) { // people
              if (health != Deceased) {
                if ((health != Susceptible) && (health != Recover)) {
                    if (daysInfected == stateDuration(health)) {
                        // health = 4
                        health = change(health, vulnerability)
                        daysInfected = 0
                    } else {
                        daysInfected = daysInfected + 1
                    }
                }

                while (messages.hasNext) {
                    val m: Double = messages.next.head
                    if (health==0) {
                      var risk: Double = m
                      if (age > 60) {
                        risk = risk * 2
                      } 
                      if (risk > 1) {
                        health = change(health, vulnerability)
                      }
                    }
                }

                val SymptomaticBool: Boolean = if (symptomatic == 1) true else false
                // Calculate infectiousness once
                val infectious: Double = infectiousness(health.toInt, SymptomaticBool)

                val it = getEdges.iterator()
                while (it.hasNext) {
                  val edge = it.next
                  Range(0, cfreq).foreach(i => 
                    sendMessageTo(edge.getTarget, Array(infectious))
                  )
                }
              }
          } else {  // clock vertex
            // setNewVertexValue(state)
            if (idleCountDown > 1) {
              idleCountDown -= 1
              sendMessageTo(vertex.getId(), Array(0))
            } else {
              idleCountDown = interval
              val it = getEdges.iterator()
              while (it.hasNext) {
                val edge = it.next
                sendMessageTo(edge.getTarget, Array(0))
              }
          }
        }
        setNewVertexValue(Array(age, symptomatic, health, vulnerability, daysInfected, idleCountDown))            
      }
    }

    // message combiner
    // combinedMessage has the same type as Message
    final class EpidemicsCombiner extends MessageCombiner[LongValue, Array[Double]] {

        override def combineMessages(messages: MessageIterator[Array[Double]]): Unit = {

            var combined: Array[Double] = Array()
            while (messages.hasNext) {
              val msg = messages.next
              combined = msg ++ combined
            }

            sendCombinedMessage(combined)
        }
    }

    measure(() => {
      val result = graph.runVertexCentricIteration(new EpidemicsComputeFunction(), new EpidemicsCombiner(), maxIterations)
      val simResult = result.getVertices
      simResult.collect()
    }, maxIterations)
  }
}
