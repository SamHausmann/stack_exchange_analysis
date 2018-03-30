package XMLParse

import scala.collection.mutable.ListBuffer

// Class to store information related to a specific User
object Utility {

  def AddStringToListBuffer(list: ListBuffer[String], string: String) : ListBuffer[String] = {
    list += string
    list
  }

  def CombineStringBuffers(list1: ListBuffer[String], list2: ListBuffer[String]) : ListBuffer[String] = {
    list1.appendAll(list2)
    list1
  }
}