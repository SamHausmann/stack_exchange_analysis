package XMLParse

case class Answer(AnswerId: Int,
                ParentId: Int,
                AnswerCreationDate: Long,
                Score: Int,
                ViewCount: Int,
                Body: String,
                OwnerUserId: Int,
                CommentCount: Int,
                FavoriteCount: Int)

object Answers extends BaseFile {

  private[XMLParse] def Extract(post: Post): Answer = {
    Answer(
      post.Id,
      post.ParentId.get,
      post.CreationDate,
      post.Score,
      post.ViewCount.getOrElse(0),
      post.Body,
      post.OwnerUserId,
      post.CommentCount.getOrElse(0),
      post.FavoriteCount.getOrElse(0))
  }
}