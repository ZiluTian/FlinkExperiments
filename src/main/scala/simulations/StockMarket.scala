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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.java.io.TextInputFormat

object StockMarket {
  def main(args: Array[String]): Unit = {
	  // val LOG: Logger = LoggerFactory.getLogger("root")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(args(0).toInt)
    val edgeFilePath = args(1)
    
    // define the maximum number of iterations
    val maxIterations = 200

    // ===========Load edge file and create the graph =========
    // Graph<K, VV, EV>    
    // Update the path to the input edge file 
    val edgeDataset: DataSet[Edge[LongValue, List[Double]]] = env.readTextFile(edgeFilePath).map(
      new MapFunction[String, Edge[LongValue, List[Double]]]() { 
        def map(value: String): Edge[LongValue, List[Double]] = { 
          val fields = value.split(" ")
          val srcId: LongValue = new LongValue(fields(0).toLong)
          val targetId: LongValue = new LongValue(fields(1).toLong)
          new Edge(srcId, targetId, List(0.0))
        }
    })

    val graph: Graph[LongValue, List[List[Double]], List[Double]] = Graph.fromDataSet(edgeDataset, env).mapVertices( 
      new MapFunction[Vertex[LongValue, NullValue], List[List[Double]]]() { 
        def map(value: Vertex[LongValue, NullValue]): List[List[Double]] =  {
            val stock_timeseries = List(1000.0)
            val marketState = List(0, 1000, 1000.0, 0, 0, 0, 0)
            val timer = List(0.0)   // for calculating past average
            val traderState = List(1000, 1, 1000.0)
            val rules = List(0, 0, 0, 0, 0.0, Random.nextInt(5), 0)   // 5 rules and their respective strength, initially 0; most recent rule
            List(stock_timeseries, marketState, timer, traderState, rules)
        }
    })

    // ===========Simulation-specific logic =========

    val priceAdjustmentFactor = 0.01
    val interestRate = 0.0001

    def update(window: Double, timer: Double, lastAvg: Double, stock_timeseries: List[Double]): Int = {
        // moving window    
        var calculated_avg: Double = -1
        var sumPrice: Double = 0

        if (timer > window) {
            var i: Int = (timer-window).toInt
            while(i<timer){
                sumPrice = stock_timeseries(i) + sumPrice  
                i += 1
            }
            calculated_avg = sumPrice/window;
        } else {
            var i = 0
            while (i<timer){
                sumPrice += stock_timeseries(i)
                i += 1  
            }
            calculated_avg = sumPrice/timer;
        }

        if (lastAvg < calculated_avg){
            1
        } else {
            0
        }
    }

    // return (action, cash, shares)
    def evalRule(rule: Int, stockPrice: Double, marketState: List[Double], cash: Double, shares: Double): (Int, Double, Double) = {
        val lastDividend = marketState(0)
        val lastAvg = marketState(1)
        val currentPrice = marketState(2)
        val dividendIncrease = marketState(3)
        val recent10AvgInc = marketState(4)
        val recent50AvgInc = marketState(5)   
        val recent100AvgInc = marketState(6)    

        var action = 0
        val buy = 1
        val sell = 2

        rule match {
            case 1 => 
                if (dividendIncrease == 1 && stockPrice < cash) {
                    action = buy
                } else if (dividendIncrease == 2 && shares > 1) {
                    action = sell
                } 
            case 2 =>
                if (recent10AvgInc == 1 && shares >= 1){
                    action = sell
                } else if (stockPrice < cash && recent10AvgInc == 2){
                    action = buy
                } 
            case 3 =>
                if (recent50AvgInc == 1 && shares >= 1){
                    action = sell
                } else if (stockPrice < cash && recent50AvgInc == 2){
                    action = buy
                } 
            case 4 =>
                if (recent100AvgInc == 1 && shares >= 1){
                    action = sell
                } else if (stockPrice < cash && recent100AvgInc == 2){
                    action = buy
                } 
            case _ => 
                if (Random.nextBoolean){
                    if (stockPrice < cash) {
                        action = buy
                    } 
                } else {
                    if (shares > 1) {
                        action = sell
                    }
                }
        }
        if (action == buy) {
            (buy, cash - stockPrice, shares + 1)
        } else if (action == sell) {
            (sell, cash + stockPrice, shares - 1)
        } else {
            (0, cash, shares)
        }
    }

    // K, V, E, M
    final class StockMarketComputeFunction extends ComputeFunction[LongValue, List[List[Double]], List[Double], List[List[Double]]] {

        override def compute(vertex: Vertex[LongValue, List[List[Double]]], messages: MessageIterator[List[List[Double]]]): Unit = {
            val state: List[List[Double]] = vertex.getValue()
            var stock_timeseries = state(0)
            var marketState = state(1)
            var lastDividend = marketState(0)
            var lastAvg = marketState(1)
            var currentPrice = marketState(2)
            var dividendIncrease = marketState(3)
            var recent10AvgInc = marketState(4)
            var recent50AvgInc = marketState(5)    
            var recent100AvgInc = marketState(6)       
            var timer = state(2).head
            var traderState = state(3)
            var cash = traderState(0)
            var shares = traderState(1)
            var estimatedWealth = traderState(2)

            var rules = state(4).toBuffer
            var lastRule: Int = rules(5).toInt
            var nextAction: Int = rules(6).toInt

            timer += 1 
            if (vertex.getId() != 0) {  // trader 
                cash = cash * (1 + interestRate)
                messages.forEach(m => {
                    val ms: List[Double] = m.asInstanceOf[List[Double]]
                    val m_dividendPerShare = ms(0)
                    val m_lastAvg = ms(1)
                    val m_currentPrice = ms(2)
                    val m_dividendIncrease = ms(3)
                    val m_recent10AvgInc = ms(4)
                    val m_recent50AvgInc = ms(5)    
                    val m_recent100AvgInc = ms(6)    
                    val previousWealth = estimatedWealth
                    // Update the number of shares based on the new dividend
                    shares = shares * (1 + m_dividendPerShare)
                    // Calculate the new estimated wealth 
                    estimatedWealth = cash + shares * m_currentPrice
                    // Update the strength of prev action based on the feedback of the wealth changes
                    if (estimatedWealth > previousWealth) {
                        rules(lastRule) += 1
                    } else if (estimatedWealth < previousWealth) {
                        rules(lastRule) -= 1
                    }
                    // Select the rule with the highest strength for the next action 
                    val nextRule = rules.zipWithIndex.toList.sortBy(x => x._1).head._2
                    // Obtain the action based on the rule 
                    val x = evalRule(nextRule, m_currentPrice, ms, cash, shares)
                    // Update lastRule with the recently selected rule 
                    rules(5) = nextRule
                    // Update the last action, cash, and shares
                    rules(6) = x._1
                    cash = x._2
                    shares = x._3 

                    val it = getEdges.iterator()
                    while (it.hasNext) {
                        val edge = it.next
                        sendMessageTo(edge.getTarget, List(List(rules(6))))
                    }
                })
            } else {    // market
                var buyOrders: Int = 0
                var sellOrders: Int = 0

                messages.forEach(m => {
                    val ms: List[Double] = m.asInstanceOf[List[Double]]
                    if (ms(0)==1) {
                        buyOrders = buyOrders + 1
                    } else if (ms(0)==2) {
                        sellOrders = sellOrders + 1
                    }
                })
                // Update price based on buy-sell orders
                currentPrice = currentPrice*(1+priceAdjustmentFactor*(buyOrders - sellOrders))
                // Update the stock time series with the new price
                stock_timeseries = stock_timeseries ::: List(currentPrice)
                // Increment the timer 
                // Calculate the average of the stock price
                lastAvg = stock_timeseries.reduce((a, b) => a + b) / timer 
                // Calculate new dividend
                var newDividendPerShare = 0.1* Random.nextGaussian()
                if (newDividendPerShare < 0) {
                    newDividendPerShare = 0
                }
                // Calculate whether dividend has increased. 0: None, 1: true, 2: false
                var dividendIncrease = 1.0
                if (newDividendPerShare == 0) {
                    dividendIncrease = 0
                } else if (lastDividend > newDividendPerShare) {
                    dividendIncrease = 2
                }
                // Calculate whether avg has increased for past 10 rounds
                recent10AvgInc = update(10, timer, lastAvg, stock_timeseries)
                // Calculate whether avg has increased for past 50 rounds
                recent50AvgInc = update(50, timer, lastAvg, stock_timeseries)
                // Calculate whether avg has increased for past 100 rounds
                recent100AvgInc = update(100, timer, lastAvg, stock_timeseries)
                // Send messages to traders
                val it = getEdges.iterator()
                while (it.hasNext) {
                    val edge = it.next
                    sendMessageTo(edge.getTarget, List(List(lastDividend, lastAvg, currentPrice, dividendIncrease, recent10AvgInc, recent50AvgInc, recent100AvgInc)))
                }
            }
            setNewVertexValue(List(stock_timeseries, List(lastDividend, lastAvg, currentPrice, dividendIncrease, recent10AvgInc, recent50AvgInc, recent100AvgInc), List(timer), 
            List(cash, shares, estimatedWealth), rules.toList))
      }
    }

    // message combiner
    // combinedMessage has the same type as Message
    final class StockMarketCombiner extends MessageCombiner[LongValue, List[List[Double]]] {

        override def combineMessages(messages: MessageIterator[List[List[Double]]]): Unit = {
            var combined: List[List[Double]] = List()

            while (messages.hasNext) {
              val msg = messages.next
              combined = msg ::: combined
            }
            // LOG.info("Combined value is " + combined + " total messages " + totalMsgs)
            sendCombinedMessage(combined)
        }
    }

    // // // Execute the vertex-centric iteration
    val result = graph.runVertexCentricIteration(new StockMarketComputeFunction(), new StockMarketCombiner(), maxIterations)

    // // // Extract the vertices as the result
    val golResult = result.getVertices

    // // execute
    golResult.collect()
  }
}
