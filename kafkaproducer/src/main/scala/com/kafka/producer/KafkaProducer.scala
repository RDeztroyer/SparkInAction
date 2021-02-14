package com.kafka.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.codehaus.jackson.map.ObjectMapper

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.{Properties, UUID}

object KafkaProducer extends App {

  val topic = "topic1"
  val env = "test"
  val numberOfTimes = 1

  val topicName = topic match {
    case "topic1" => "TOPIC_NAME_TO_SEND_MESSAGES"
    case "topic2" => "TOPIC_NAME_TO_SEND_MESSAGES"
  }

  var props:Properties = new Properties()

  env match {
    case "stage" =>     props.put("zookeeper.consumer.connection", "xxx:2181")
                        props.put("bootstrap.servers", "xxxx:9092")
    case "test"  =>     props.put("zookeeper.consumer.connection", "xxxx:2181")
                        props.put("bootstrap.servers", "xxxx:9092")
    case "prod"  =>     props.put("zookeeper.consumer.connection", "xxxx:2181")
                        props.put("bootstrap.servers", "xxxx:9092")
  }

  props.put("acks", "all")
  props.put("retries", 0:Integer)
  //props.put("batch.size", 16384:Integer)
  props.put("linger.ms", 1:Integer)
  //props.put("buffer.memory", 33554432:Integer)
  props.put("max.request.size", 101943040:Integer)
  props.put("zookeeper.consumer.path", "/kafka")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val mapper = new ObjectMapper


  println(s"files\\${topic}\\")
  val folder = new File(s"files\\${topic}\\")
  for (i <- 0 until numberOfTimes) {
    for (fileEntry <- folder.listFiles) {
      val message = new String(Files.readAllBytes(Paths.get(fileEntry.getAbsolutePath)))
      val _UUID = UUID.randomUUID.toString
      val messageToPush = mapper.writeValueAsString(message)
      System.out.println(messageToPush)
      val x = producer.send(new ProducerRecord[String, String](topicName, _UUID, messageToPush))
      System.out.println(x.get.offset)
      System.out.println("Message sent successfully")
    }
  }

  producer.close()
}
