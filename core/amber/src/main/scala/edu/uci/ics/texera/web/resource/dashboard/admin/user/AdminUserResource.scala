/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.texera.web.resource.dashboard.admin.user

import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.EmailTemplate.createRoleChangeTemplate
import edu.uci.ics.texera.web.resource.GmailResource.sendEmail
import edu.uci.ics.texera.web.resource.dashboard.admin.user.AdminUserResource.userDao
import edu.uci.ics.texera.web.resource.dashboard.user.quota.UserQuotaResource._
import org.jasypt.util.password.StrongPasswordEncryptor
import edu.uci.ics.texera.dao.jooq.generated.tables.User.USER
import edu.uci.ics.texera.dao.jooq.generated.tables.UserLastActiveTime.USER_LAST_ACTIVE_TIME

import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

case class UserInfo(
    uid: Int,
    name: String,
    email: String,
    googleId: String,
    role: UserRoleEnum,
    googleAvatar: String,
    comment: String,
    lastLogin: java.time.OffsetDateTime, // will be null if never logged in
    accountCreation: java.time.OffsetDateTime
)

object AdminUserResource {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()
  final private lazy val userDao = new UserDao(context.configuration)
}

@Path("/admin/user")
@RolesAllowed(Array("ADMIN"))
class AdminUserResource {

  /**
    * This method returns the list of users
    *
    * @return a list of UserInfo
    */
  @GET
  @Path("/list")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def list(): util.List[UserInfo] = {
    AdminUserResource.context
      .select(
        USER.UID,
        USER.NAME,
        USER.EMAIL,
        USER.GOOGLE_ID,
        USER.ROLE,
        USER.GOOGLE_AVATAR,
        USER.COMMENT,
        USER_LAST_ACTIVE_TIME.LAST_ACTIVE_TIME,
        USER.ACCOUNT_CREATION_TIME
      )
      .from(USER)
      .leftJoin(USER_LAST_ACTIVE_TIME)
      .on(USER.UID.eq(USER_LAST_ACTIVE_TIME.UID))
      .fetchInto(classOf[UserInfo])
  }

  @PUT
  @Path("/update")
  def updateUser(user: User): Unit = {
    val existingUser = userDao.fetchOneByEmail(user.getEmail)
    if (existingUser != null && existingUser.getUid != user.getUid) {
      throw new WebApplicationException("Email already exists", Response.Status.CONFLICT)
    }
    val updatedUser = userDao.fetchOneByUid(user.getUid)
    val roleChanged = updatedUser.getRole != user.getRole
    updatedUser.setName(user.getName)
    updatedUser.setEmail(user.getEmail)
    updatedUser.setRole(user.getRole)
    updatedUser.setComment(user.getComment)
    userDao.update(updatedUser)

    if (roleChanged)
      sendEmail(
        createRoleChangeTemplate(receiverEmail = updatedUser.getEmail, newRole = user.getRole),
        updatedUser.getEmail
      )
  }

  @POST
  @Path("/add")
  def addUser(): Unit = {
    val random = System.currentTimeMillis().toString
    val newUser = new User
    newUser.setName("User" + random)
    newUser.setPassword(new StrongPasswordEncryptor().encryptPassword(random))
    newUser.setRole(UserRoleEnum.INACTIVE)
    userDao.insert(newUser)
  }

  @GET
  @Path("/created_workflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCreatedWorkflow(@QueryParam("user_id") user_id: Integer): List[Workflow] = {
    getUserCreatedWorkflow(user_id)
  }

  @GET
  @Path("/access_workflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAccessedWorkflow(@QueryParam("user_id") user_id: Integer): util.List[Integer] = {
    getUserAccessedWorkflow(user_id)
  }

  @GET
  @Path("/user_quota_size")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getUserQuota(@QueryParam("user_id") user_id: Integer): Array[QuotaStorage] = {
    getUserQuotaSize(user_id)
  }

  @DELETE
  @Path("/deleteCollection/{eid}")
  def deleteCollection(@PathParam("eid") eid: Integer): Unit = {
    deleteExecutionCollection(eid)
  }
}
