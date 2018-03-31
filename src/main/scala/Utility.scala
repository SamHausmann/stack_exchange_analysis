package XMLParse

import XMLParse.Badges.importantBadges

import scala.collection.mutable.ListBuffer

// Class to store information related to a specific User
object Utility {

  def AddToListBuffer[T](list: ListBuffer[T], item: T) : ListBuffer[T] = {
    list += item
    list
  }

  def CombineBuffers[T](list1: ListBuffer[T], list2: ListBuffer[T]) : ListBuffer[T] = {
    list1.appendAll(list2)
    list1
  }

  // Count the number of occurences of each item in the compare list
  // ex.   (1, 2, 3, 3, 1) (1, 2, 3) -> (2, 1, 2)
  private[XMLParse] def MapListOfItemsToCounts[T](inputList: List[T], compareList: List[T]): List[Int] = {
    compareList.map(item => inputList.count(_ == item))
  }
}