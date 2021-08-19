package ru.otus.jdbc.dao.slick



import ru.otus.jdbc.model.{Role, User}
import slick.dbio.Effect
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.TransactionIsolation
import slick.sql.FixedSqlAction

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}




class UserDaoSlickImpl(db: Database)(implicit ec: ExecutionContext) {
  import UserDaoSlickImpl._

  def getUser(userId: UUID): Future[Option[User]] = {
    val res = for {
      user  <- users.filter(user => user.id === userId).result.headOption
      roles <- usersToRoles.filter(_.usersId === userId).map(_.rolesCode).result.map(_.toSet)
    } yield user.map(_.toUser(roles))

    db.run(res)
  }

  def createUser(user: User): Future[User] = {
    val action = for {
      id <- ((users returning users.map(_.id)) += UserRow.fromUser(user.copy(id = None)))
      _ <- usersToRoles ++= user.roles.map(id -> _)
    } yield user.copy(id = Some(id))

    db.run(action.transactionally)
  }

  def updateUser(user: User): Future[Unit] = {
    user.id match {
      case Some(userId) =>
        val updateUser = users
            .filter(_.id === userId)
            .map(u => (u.firstName, u.lastName, u.age))
            .update((user.firstName, user.lastName, user.age))

        val deleteRoles = usersToRoles.filter(_.usersId === userId).delete
        val insertRoles = usersToRoles ++= user.roles.map(userId -> _)

        val action = updateUser >> deleteRoles >> insertRoles >> DBIO.successful(())

        db.run(action.transactionally.withTransactionIsolation(TransactionIsolation.Serializable))
      case None => Future.successful(())
    }
  }

  def deleteUser(userId: UUID): Future[Option[User]] = {
    for {
      user <- getUser(userId)
      action = usersToRoles.filter(_.usersId === userId).delete >> users.filter(_.id === userId).delete
      _ <- db.run(action.transactionally) if (user.nonEmpty)
    } yield user
  }

  private def findByCondition(condition: Users => Rep[Boolean]): Future[Vector[User]] = {
    val findAction = users.filter(condition(_))
    val action = for {
      users <- findAction.result
      roles <- usersToRoles.filter(_.usersId in findAction.map(_.id)).result
    } yield users.map(u => u.toUser(roles.collect({ case (id, r) if id == u.id.get => r }).toSet)).toVector

    db.run(action)
  }

  def findByLastName(lastName: String): Future[Seq[User]] = findByCondition(_.lastName === lastName)

  def findAll(): Future[Seq[User]] = findByCondition(_ => true)

  private[jdbc] def deleteAll(): Future[Unit] = db.run((usersToRoles.delete >> users.delete >> DBIO.successful(())).transactionally)
}

object UserDaoSlickImpl {
  implicit val rolesType: BaseColumnType[Role] = MappedColumnType.base[Role, String](
    {
      case Role.Reader => "reader"
      case Role.Manager => "manager"
      case Role.Admin => "admin"
    },
    {
        case "reader"  => Role.Reader
        case "manager" => Role.Manager
        case "admin"   => Role.Admin
    }
  )


  case class UserRow(
      id: Option[UUID],
      firstName: String,
      lastName: String,
      age: Int
  ) {
    def toUser(roles: Set[Role]): User = User(id, firstName, lastName, age, roles)
  }

  object UserRow extends ((Option[UUID], String, String, Int) => UserRow) {
    def fromUser(user: User): UserRow = UserRow(user.id, user.firstName, user.lastName, user.age)
  }

  class Users(tag: Tag) extends Table[UserRow](tag, "users") {
    val id        = column[UUID]("id", O.PrimaryKey, O.AutoInc)
    val firstName = column[String]("first_name")
    val lastName  = column[String]("last_name")
    val age       = column[Int]("age")

    val * = (id.?, firstName, lastName, age).mapTo[UserRow]
  }

  val users: TableQuery[Users] = TableQuery[Users]

  class UsersToRoles(tag: Tag) extends Table[(UUID, Role)](tag, "users_to_roles") {
    val usersId   = column[UUID]("users_id")
    val rolesCode = column[Role]("roles_code")

    val * = (usersId, rolesCode)
  }

  val usersToRoles = TableQuery[UsersToRoles]
}
