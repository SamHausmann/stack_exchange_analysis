package XMLParse

case class Question(QuestionId: Int,
                    QuestionCreationDate: Long)

object Questions extends BaseFile {

  private[XMLParse] def Extract(post: Post): Question = {
    Question(post.Id, post.CreationDate)
  }
}