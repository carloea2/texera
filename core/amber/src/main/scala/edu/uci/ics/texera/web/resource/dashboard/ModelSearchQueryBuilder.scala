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

package edu.uci.ics.texera.web.resource.dashboard

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.util.LakeFSStorageClient
import edu.uci.ics.texera.dao.jooq.generated.Tables.{ MODEL, MODEL_USER_ACCESS}
import edu.uci.ics.texera.dao.jooq.generated.enums.PrivilegeEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.User.USER
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{ Model, User}
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryUtils.{getContainsFilter, getDateFilter, getFullTextSearchFilter}
import edu.uci.ics.texera.web.resource.dashboard.user.model.ModelResource.DashboardModel
import org.jooq.impl.DSL
import org.jooq.{Condition, GroupField, Record, TableLike}

import scala.jdk.CollectionConverters.CollectionHasAsScala

object ModelSearchQueryBuilder extends SearchQueryBuilder with LazyLogging {
  override protected val mappedResourceSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = DSL.inline(SearchQueryBuilder.MODEL_RESOURCE_TYPE),
    name = MODEL.NAME,
    description = MODEL.DESCRIPTION,
    creationTime = MODEL.CREATION_TIME,
    mid = MODEL.MID,
    ownerId = MODEL.OWNER_UID,
    isModelPublic= MODEL.IS_PUBLIC,
    modelUserAccess = MODEL_USER_ACCESS.PRIVILEGE
  )

  /*
   * constructs the FROM clause for querying models with specific access controls.
   *
   * Parameter:
   * - uid: Integer - Represents the unique identifier of the current user.
   *  - uid is 'null' if the user is not logged in or performing a public search.
   *  - Otherwise, `uid` holds the identifier for the logged-in user.
   * - includePublic - Boolean - Specifies whether to include public models in the result.
   */
  override protected def constructFromClause(
      uid: Integer,
      params: DashboardResource.SearchQueryParams,
      includePublic: Boolean = false
  ): TableLike[_] = {
    val baseJoin = MODEL
      .leftJoin(MODEL_USER_ACCESS)
      .on(MODEL_USER_ACCESS.MID.eq(MODEL.MID))
      .leftJoin(USER)
      .on(USER.UID.eq(MODEL.OWNER_UID))

    // Default condition starts as true, ensuring all models are selected initially.
    var condition: Condition = DSL.trueCondition()

    if (uid == null) {
      // If `uid` is null, the user is not logged in or performing a public search
      // We only select models marked as public
      condition = MODEL.IS_PUBLIC.eq(true)
    } else {
      // When `uid` is present, we add a condition to only include models with direct user access.
      val userAccessCondition = MODEL_USER_ACCESS.UID.eq(uid)

      if (includePublic) {
        // If `includePublic` is true, we extend visibility to public models as well.
        condition = userAccessCondition.or(MODEL.IS_PUBLIC.eq(true))
      } else {
        condition = userAccessCondition
      }
    }
    baseJoin.where(condition)
  }

  override protected def constructWhereClause(
      uid: Integer,
      params: DashboardResource.SearchQueryParams
  ): Condition = {
    val splitKeywords = params.keywords.asScala
      .flatMap(_.split("[+\\-()<>~*@\"]"))
      .filter(_.nonEmpty)
      .toSeq

    getDateFilter(
      params.creationStartDate,
      params.creationEndDate,
      MODEL.CREATION_TIME
    )
      .and(getContainsFilter(params.modelIds, MODEL.MID))
      .and(
        getFullTextSearchFilter(splitKeywords, List(MODEL.NAME, MODEL.DESCRIPTION))
      )
  }

  override protected def getGroupByFields: Seq[GroupField] = {
    Seq.empty
  }

  override protected def toEntryImpl(
      uid: Integer,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val model = record.into(MODEL).into(classOf[Model])
    val owner = record.into(USER).into(classOf[User])
    var size = 0L

    try {
      size = LakeFSStorageClient.retrieveRepositorySize(model.getName)
    } catch {
      case e: io.lakefs.clients.sdk.ApiException =>
        // Treat all LakeFS ApiException as mismatch (repository not found, being deleted, or any fatal error)
        logger.error(s"LakeFS ApiException for model '${model.getName}': ${e.getMessage}", e)
        return null
    }

    val dd = DashboardModel(
      model,
      owner.getEmail,
      record.get(MODEL_USER_ACCESS.PRIVILEGE, classOf[PrivilegeEnum]),
      model.getOwnerUid == uid,
      size
    )
    DashboardClickableFileEntry(
      resourceType = SearchQueryBuilder.MODEL_RESOURCE_TYPE,
      model = Some(dd)
    )
  }
}

class ModelSearchQueryBuilder {}
