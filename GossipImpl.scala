package com.dos.gossip

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.math.pow
import scala.util.Random

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala

/**
 * @author Ananda Kishore Sirivella
 */
sealed trait Message
case class BuildNeighbours(neighbourActors: ArrayBuffer[ActorRef]) extends Message
case class ImplementGossip() extends Message
case class ImplementPushSum(s: Double, w: Double) extends Message
case class NodeTerminated() extends Message
case class MessagePropSuccess() extends Message
case class NeighbourTerminate() extends Message
case class SpreadPushSum() extends Message
case class PrintNeighbours() extends Message
case class SpreadGossip() extends Message
case class StartGossip() extends Message
case class BuildForPushSum(s: Double, w: Double) extends Message
case class Discard(neighbour: ActorRef) extends Message

object GossipImpl extends App{

	if(args.isEmpty || args(0).isEmpty() || args(1).isEmpty() || args(2).isEmpty()){
		println("Enter arguments for NumberOfNodes Topology Algorithm");
	}else{
		InitializeAndInitiate(args(0).toInt, args(1), args(2))
	}

	def InitializeAndInitiate(totalNumNodes: Int, topology: String, algorithm: String) 
	{
		val system = ActorSystem("InformationPropagationSystem")
				val master = system.actorOf(Props(new Master(totalNumNodes, topology, algorithm)), name = "Master")
				master ! StartGossip
	}
}

class Master (totalNumNodes: Int, topology: String, algorithm: String) extends Actor{

	var AllActors = new ArrayBuffer[ActorRef]()
			var NeighbourActors = new ArrayBuffer[ActorRef]()
			var aliveActors = new ArrayBuffer[ActorRef]()
			var numNodes = math.ceil(math.cbrt(totalNumNodes.toDouble)).toInt 
			var terminatedNodeCount: Int = 0
			var messageCounter: Int = 0
			var msgPropTime: Long = 0

			for (i <- 0 until totalNumNodes) 
				AllActors += context.actorOf(Props(new Worker(topology)), name = "AllActors" + i)        

				aliveActors ++= AllActors

				def receive = {

					// Starting point of the message propagation
				case StartGossip =>
				{
					msgPropTime = System.currentTimeMillis()

							for(i <- 0 until totalNumNodes)
							{
								BuildNeighboursForNode(i)
								AllActors(i) ! BuildForPushSum(i+1,1)
							}								

					algorithm.toLowerCase() match {

					case "gossip" =>
					{
						AllActors(Random.nextInt(totalNumNodes)) ! ImplementGossip
					}
					case "push-sum" =>
					{
						var randomNodeIndex = Random.nextInt(totalNumNodes)
								AllActors(Random.nextInt(totalNumNodes)) ! ImplementPushSum(1,0)
					}

					case _ =>
					println("Invalid Algorithm, Select either Gossip or PushSum")
					}
				}

				// Is invoked by the Workers to inform the Master that the node is terminated, also holds the exit criterion of the system
				case NodeTerminated =>
				{
					terminatedNodeCount += 1
							if(terminatedNodeCount == totalNumNodes){
								println("-------------------------------------")
								println("Convergence Time for message is " + (System.currentTimeMillis() - msgPropTime))
								println("-------------------------------------")
								context.system.shutdown()

							}else if(algorithm.toLowerCase().equalsIgnoreCase("push-sum") && terminatedNodeCount == 1){
								println("-------------------------------------")
								println("Convergence Time for message is " + (System.currentTimeMillis() - msgPropTime))
								println("-------------------------------------")
								context.system.shutdown()

							}

					if(topology.equalsIgnoreCase("imp3d")){
						if(!aliveActors.isEmpty)
						{
							aliveActors -= sender
									for(i <- 0 until aliveActors.length){
										aliveActors(i) ! Discard(sender)
									}
						}

					}
				}

				// It is used to identify if the message has been propagated through the system even though the exit criterion is not met
				case MessagePropSuccess =>
				{
					messageCounter += 1
							if(messageCounter == totalNumNodes){
								println("Message has propagated through entire system, nodes reached = " +messageCounter)
							}
				}

				// Error handling for any undefined operation
				case _ =>
				println("Undefined Operation for Master");
			}

			// Helper function to build Neighbours for each node index of the topology
			def BuildNeighboursForNode(nodeIndex: Int){

				topology.toLowerCase() match{

				case "line" =>
				{
					BuildLineTopology(nodeIndex)
					AllActors(nodeIndex) ! BuildNeighbours(NeighbourActors)
				}

				case "3d" =>
				{
					Build3DTopology(nodeIndex)
					AllActors(nodeIndex) ! BuildNeighbours(NeighbourActors)
				}

				case "imp3d" =>
				{
					Build3DTopology(nodeIndex)
					// While adding the random neighbour for the imp3d topology, it is ensured that the randomly added neighbour is not already part of the 3d neighbour set computed above
					var temporaryBuffer = new ArrayBuffer[ActorRef]
							temporaryBuffer ++= AllActors
							temporaryBuffer --= NeighbourActors
							temporaryBuffer -= AllActors(nodeIndex)

							if(!temporaryBuffer.isEmpty)
								NeighbourActors += temporaryBuffer(Random.nextInt(temporaryBuffer.length))
								temporaryBuffer.clear()
								AllActors(nodeIndex) ! BuildNeighbours(NeighbourActors)
				}

				case "full" =>
				{
					AllActors(nodeIndex) ! BuildNeighbours(AllActors - AllActors(nodeIndex))
				}

				case _ =>
				println("Unsupported topology, cannot proceed further")
				}
				NeighbourActors =  new ArrayBuffer[ActorRef]()
			}

			// Helper function to build the neighbours for Line Topology
			def BuildLineTopology(nodeIndex: Int){

				if(nodeIndex-1 >= 0)
					NeighbourActors += AllActors(nodeIndex-1)
					if(nodeIndex < totalNumNodes -1)
						NeighbourActors += AllActors(nodeIndex+1)
			}

			// Helper function to build the Neighbours for the 3D topology
			def Build3DTopology(nodeIndex: Int){

				if((nodeIndex - numNodes*numNodes) >= 0){
					NeighbourActors += AllActors(nodeIndex - numNodes*numNodes)
				}
				if((nodeIndex + numNodes*numNodes) < totalNumNodes){
					NeighbourActors += AllActors(nodeIndex + numNodes*numNodes)
				}
				if((nodeIndex-numNodes >= 0) && ((nodeIndex-numNodes)/(numNodes*numNodes)) == (nodeIndex/(numNodes*numNodes))){
					NeighbourActors += AllActors(nodeIndex-numNodes)
				}
				if((nodeIndex+numNodes < totalNumNodes) && 
						((nodeIndex+numNodes)/(numNodes*numNodes)) == (nodeIndex/(numNodes*numNodes))){
					NeighbourActors += AllActors(nodeIndex+numNodes)
				}
				if((nodeIndex-1 >= 0) &&  ((nodeIndex-1)/(numNodes)) == (nodeIndex/(numNodes))){
					NeighbourActors += AllActors(nodeIndex-1)
				}
				if( (nodeIndex+1 < totalNumNodes) && ((nodeIndex+1)/(numNodes)) == (nodeIndex/(numNodes))){
					NeighbourActors += AllActors(nodeIndex+1)
				}

			}

}

class Worker(topology: String) extends Actor{
	import context._

	var Neighbours = new ArrayBuffer[ActorRef]()
	var MasterRef: ActorRef = null
	var terminated: Boolean = false
	var firstMessage: Boolean = true
	var messageCounter: Int = 0
	var sLocal: Double = 0
	var wLocal: Double = 0
	var swCurrent: Double = 0
	var swPrevious: Double = 0
	var pushSumCounter: Int = 0

	def receive = {

			// To build the neighbours for the actor
	case BuildNeighbours(neighbourActors) =>
	{
		Neighbours ++= neighbourActors
				MasterRef = sender
	}

	// Included for Debugging purpose to validate the topology built
	case PrintNeighbours =>
	{
		var builder: StringBuilder = new StringBuilder()
	for(i <- 0 until Neighbours.length){
		builder.append(" " + Neighbours(i).toString())
	}
	println("Neighbours of " + self.toString() + " are " + builder.toString())
	}

	// Implements the Gossip Algorithm for the topology selected
	case ImplementGossip =>
	{
		if(!terminated && !Neighbours.isEmpty){
			if(firstMessage){
				firstMessage = false
						MasterRef ! MessagePropSuccess
			}
			messageCounter += 1
					if(messageCounter >= 10 || Neighbours.isEmpty)
					{
						terminated = true
								destruct
					}
			self ! SpreadGossip

		}
	}

	// Implements the Push sum algorithm for the topology selected
	case ImplementPushSum(s,w) =>
	{
		sLocal += s
				wLocal += w
				swCurrent = sLocal/wLocal
				calculateConvergence
				if(!terminated){
					swPrevious = swCurrent
							self ! SpreadPushSum
				}

	}

	// Recursively spreads the push sum message across the neighbours in the topology
	case SpreadPushSum =>
	{
		sLocal /=2
				wLocal /=2
				if(!terminated && !Neighbours.isEmpty){
					Neighbours(Random.nextInt(Neighbours.length)) ! ImplementPushSum(sLocal, wLocal)
					context.system.scheduler.scheduleOnce(50 milliseconds,self, SpreadPushSum)      
				}		
	}

	// Recursively spreads the gossip message across the neighbours in the topology
	case SpreadGossip =>
	{
		if(!terminated && !Neighbours.isEmpty){
			Neighbours(Random.nextInt(Neighbours.length)) ! ImplementGossip
			context.system.scheduler.scheduleOnce(50 milliseconds, self, SpreadGossip)
		}else{
			destruct
		}
	}

	// Is invoked by the neighbours to inform the Actor that it is no longer part of its neighbour system
	case NeighbourTerminate =>
	{
		if(!Neighbours.isEmpty && !terminated)
		{
			Neighbours -= sender
					if(Neighbours.isEmpty)
						destruct
		}
	}

	case BuildForPushSum(s,w) =>
	{
		sLocal = s
				wLocal = w
	}

	case Discard(neighbour) =>
	{
		if(!terminated && !Neighbours.isEmpty){
			Neighbours -= neighbour
					if(Neighbours.isEmpty)
						destruct
		}
	}
	// Error handling for any undefined operation
	case _ =>
	println("Undefined operation for Worker");
	}

	// Destruct is invoked if the exit criterion for the Actor is met, it informs the Master and Neighbours that it has exited
	def destruct = {
			terminated = true
					MasterRef ! NodeTerminated
					if(!Neighbours.isEmpty){
						for(i <- 0 until Neighbours.length)
							Neighbours(i) ! NeighbourTerminate
					}
			context.stop(self)
	}


	// Used to calculate the convergence for Push sum and defines the exit criterion
	def calculateConvergence = {

			if(math.abs(swCurrent - swPrevious) <= 0.0000000001){
				pushSumCounter +=1 
						if(pushSumCounter >= 3){
							destruct
						}
			}else{
				pushSumCounter = 0
			}
	}

}