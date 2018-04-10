package XMLParse

import scala.collection.mutable.ListBuffer

// Utility Class for functions used multiple times across classes
object Utility {

  def AddToListBuffer[T](list: ListBuffer[T], item: T) : ListBuffer[T] = {
    list += item
    list
  }

  def CombineBuffers[T](list1: ListBuffer[T], list2: ListBuffer[T]) : ListBuffer[T] = {
    list1.appendAll(list2)
    list1
  }

  // Count the number of occurrences of each item in the compare list
  // ex.   (1, 2, 3, 3, 1) (1, 2, 3) -> (2, 1, 2)
  // ex.   (s1, s2, s3, s3, s1) (s1, s2, s3) -> (2, 1, 2)   // s1 represents a string
  private[XMLParse] def MapListOfItemsToCounts[T](inputList: List[T], compareList: List[T]): List[Int] = {
    compareList.map(item => inputList.count(_ == item))
  }
}