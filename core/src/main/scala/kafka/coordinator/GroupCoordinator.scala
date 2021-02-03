/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{OffsetFetchResponse, JoinGroupRequest}

import scala.collection.{Map, Seq, immutable}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           errorCode: Short)

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
  * 处理以下请求：
  * ApiKeys.OFFSET_COMMIT;
  * ApiKeys.OFFSET_FETCH;
  * ApiKeys.JOIN_GROUP;
  * ApiKeys.LEAVE_GROUP;
  * ApiKeys.SYNC_GROUP;
  * ApiKeys.DESCRIBE_GROUPS;
  * ApiKeys.LIST_GROUPS;
  * ApiKeys.HEARTBEAT;
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = (Array[Byte], Short) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  // GroupCoordinator状态标识符
  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup() {
    info("Starting up.")
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  /**
    * 处理JOIN_GROUP请求
    * @param groupId 组ID
    * @param memberId 成员ID
    * @param clientId 客户端ID
    * @param clientHost consumer的HOST
    * @param sessionTimeoutMs consumer的session超时时间
    * @param protocolType consumer
    * @param protocols 元组集合(assignor.name(),List<Subscription>)
    * @param responseCallback // 加入Group的回调
    */
  def handleJoinGroup(groupId: String,
                      memberId: String,
                      clientId: String,
                      clientHost: String,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback) {
    // 各种校验
    if (!isActive.get) {
      responseCallback(joinError(memberId, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!validGroupId(groupId)) {
      responseCallback(joinError(memberId, Errors.INVALID_GROUP_ID.code))
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(joinError(memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(joinError(memberId, Errors.GROUP_LOAD_IN_PROGRESS.code))
    } else if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
               sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT.code))
    } else {
      // only try to create the group if the group is not unknown AND
      // the member id is UNKNOWN, if member is specified but group does not
      // exist we should reject the request
      // 根据组ID找到对应的组
      var group = groupManager.getGroup(groupId)
      if (group == null) {// 组不存在
        // 成员已经加入组后又发送重复请求加入一个不存在的Group
        if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
          // 返回错误信息
          responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
        } else {
          // 创建组并保存到groupManager中，初始Group的元数据GroupMetadata状态为Stable
          group = groupManager.addGroup(new GroupMetadata(groupId, protocolType))
          // 加入组
          doJoinGroup(group, memberId, clientId, clientHost, sessionTimeoutMs, protocolType, protocols, responseCallback)
        }
      } else {// 组已经存在
        // 加入组
        doJoinGroup(group, memberId, clientId, clientHost, sessionTimeoutMs, protocolType, protocols, responseCallback)
      }
    }
  }

  /**
    * kafkaConsumer加入组
    */
  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    group synchronized {
      if (group.protocolType != protocolType || !group.supportsProtocols(protocols.map(_._1).toSet)) {
        // if the new member does not support the group protocol, reject it
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL.code))
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // if the member trying to register with a un-recognized id, send the response to let
        // it reset its member id and retry
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
      } else {
        group.currentState match {
          // 当前Group已经关闭
          case Dead =>
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
          // 当前Group准备Rebalance时的状态
          case PreparingRebalance =>
            // 如果成员ID为UNKNOWN_MEMBER_ID，加入组
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback)
            } else {
              // 如果成员ID不为UNKNOWN_MEMBER_ID，更新组
              val member = group.get(memberId)
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }
          // 当前Group准备接受Sync_Group请求时的状态
            // 也就是当前组已经开始了Rebalance，正在等待leader节点的分配结果
          case AwaitingSync =>
            // 如果成员ID为UNKNOWN_MEMBER_ID，加入组
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              if (member.matches(protocols)) {
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                responseCallback(JoinGroupResult(
                  members = if (memberId == group.leaderId) {
                    group.currentMemberMetadata
                  } else {
                    Map.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocol,
                  leaderId = group.leaderId,
                  errorCode = Errors.NONE.code))
              } else {
                // member has changed metadata, so force a rebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }
            }
          // 当Group初始化或者稳定时的状态
          case Stable =>
            // 条件成立说明成员是首次加入这个Group
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              // if the member id is unknown, register the member to the group、
              // 将成员加入组
              addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback)
            // 执行到这里说明成员是重复加入这个Group
            } else {
              // 成员ID
              val member = group.get(memberId)
              // 如果是leader节点或者成员元数据发送了变更
              if (memberId == group.leaderId || !member.matches(protocols)) {
                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                // The latter allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                // 更新组成员并执行Rebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              } else {
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                // 对于组中的非leader节点，返回当前Group的信息，然后等待他们发送SyncGroup请求
                responseCallback(JoinGroupResult(
                  members = Map.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocol,
                  leaderId = group.leaderId,
                  errorCode = Errors.NONE.code))
              }
            }
        }
        // 如果Group的状态为PreparingRebalance
        if (group.is(PreparingRebalance))
          // 尝试执行当前Group的Rebalance操作
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))
      }
    }
  }

  // 处理SYNC_GROUP请求
  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback) {
    if (!isActive.get) {
      responseCallback(Array.empty, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Array.empty, Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else {
      // 获取Group的元数据
      val group = groupManager.getGroup(groupId)
      // Group为null，返回错误响应
      if (group == null)
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
      else
      // 真正处理SYNC_GROUP请求
        doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
    }
  }

  // 真正处理SYNC_GROUP请求,leader会把分配方案发送过来，follower则发送了一个空
  private def doSyncGroup(group: GroupMetadata,// Group的元数据
                          generationId: Int,// 当前Group的"代"
                          memberId: String,// 成员ID，有可能是leader，有可能是follower
                          groupAssignment: Map[String, Array[Byte]],// 分配方案，leader发过来的：key是memberId，value是序列化后的Assignment。follow发过来空map
                          responseCallback: SyncCallback) {// 响应的回调
    var delayedGroupStore: Option[DelayedStore] = None

    group synchronized {
      if (!group.has(memberId)) {
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
      } else if (generationId != group.generationId) {
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION.code)
      } else {
        group.currentState match {
            // Group状态不对
          case Dead =>
            responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
          // Group状态不对
          case PreparingRebalance =>
            responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS.code)
          // Group状态正常
          case AwaitingSync =>
            // 设置成员的Sycn Group请求回调
            group.get(memberId).awaitingSyncCallback = responseCallback
            // 为给定的Group每一个成员重置DelayedHeartbeat定时任务
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))

            // if this is the leader, then we can attempt to persist state and transition to stable
            // 如果是leader，那么可以将Group的状态过渡到Stable
            if (memberId == group.leaderId) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              // missing members = Group成员ID - 分配方案里的Group成员ID
              val missing = group.allMembers -- groupAssignment.keySet
              // 1.missing.map(_ -> Array.empty[Byte]).toMap 为没有分配方案的Group成员，设置一个空的集合
              // 2.assignment = 分配方案groupAssignment + missing members的空的分配方案
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap
              // A.prepareStoreGroup()准备消息
              delayedGroupStore = Some(groupManager.prepareStoreGroup(group, assignment, (errorCode: Short) => {
                // C.存储完消息的回调
                group synchronized {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the AwaitingSync state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(AwaitingSync) && generationId == group.generationId) {
                    // 分配方案存储失败，重置所有成员的分配方案，之后重新Rebalance
                    if (errorCode != Errors.NONE.code) {
                      resetAndPropagateAssignmentError(group, errorCode)
                      maybePrepareRebalance(group)
                    } else {
                      // 下发分区分配结果
                      setAndPropagateAssignment(group, assignment)
                      // 修改状态为Stable
                      group.transitionTo(Stable)
                    }
                  }
                }
              }))
            }
          // Group的状态已经为Stable，要么刚刚创建，要么已经早就分区分配完毕了
          // 那么返回这个成员对应的元数据中的分配结果
          case Stable =>
            // if the group is stable, we just return the current assignment
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE.code)
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }

    // store the group metadata without holding the group lock to avoid the potential
    // for deadlock when the callback is invoked
    // B.存储消息
    delayedGroupStore.foreach(groupManager.store)
  }

  def handleLeaveGroup(groupId: String, consumerId: String, responseCallback: Short => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(Errors.GROUP_LOAD_IN_PROGRESS.code)
    } else {
      val group = groupManager.getGroup(groupId)
      if (group == null) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (!group.has(consumerId)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else {
            val member = group.get(consumerId)
            removeHeartbeatForLeavingMember(group, member)
            onMemberFailure(group, member)
            responseCallback(Errors.NONE.code)
          }
        }
      }
    }
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Short => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      // the group is still loading, so respond just blindly
      responseCallback(Errors.NONE.code)
    } else {
      val group = groupManager.getGroup(groupId)
      if (group == null) {
        responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (!group.is(Stable)) {
            responseCallback(Errors.REBALANCE_IN_PROGRESS.code)
          } else if (!group.has(memberId)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (generationId != group.generationId) {
            responseCallback(Errors.ILLEGAL_GENERATION.code)
          } else {
            val member = group.get(memberId)
            // 重置下次心跳检查的时间
            completeAndScheduleNextHeartbeatExpiration(group, member)
            responseCallback(Errors.NONE.code)
          }
        }
      }
    }
  }

  // 处理OFFSET_COMMIT请求
  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Short] => Unit) {
    var delayedOffsetStore: Option[DelayedStore] = None

    if (!isActive.get) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_LOAD_IN_PROGRESS.code))
    } else {
      val group = groupManager.getGroup(groupId)
      if (group == null) {
        if (generationId < 0)
          // the group is not relying on Kafka for partition management, so allow the commit
          delayedOffsetStore = Some(groupManager.prepareStoreOffsets(groupId, memberId, generationId, offsetMetadata,
            responseCallback))
        else
          // the group has failed over to this coordinator (which will be handled in KAFKA-2017),
          // or this is a request coming from an older generation. either way, reject the commit
          responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
          } else if (group.is(AwaitingSync)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS.code))
          } else if (!group.has(memberId)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
          } else if (generationId != group.generationId) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
          } else {
            val member = group.get(memberId)
            completeAndScheduleNextHeartbeatExpiration(group, member)
            // 准备存储offset
            delayedOffsetStore = Some(groupManager.prepareStoreOffsets(groupId, memberId, generationId,
              offsetMetadata, responseCallback))
          }
        }
      }
    }

    // store the offsets without holding the group lock
    // 存储每一条offset消息到LogSegment中
    delayedOffsetStore.foreach(groupManager.store)
  }

  /**
    * 处理OFFSET_FETCH请求
    * 拉取topic-partition的offset
    * @param groupId 组ID
    * @param partitions topic-partition
    * @return
    */
  def handleFetchOffsets(groupId: String,
                         partitions: Seq[TopicPartition]): Map[TopicPartition, OffsetFetchResponse.PartitionData] = {
    if (!isActive.get) {
      partitions.map { case topicPartition =>
        (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))}.toMap
    } else if (!isCoordinatorForGroup(groupId)) {
      partitions.map { case topicPartition =>
        (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NOT_COORDINATOR_FOR_GROUP.code))}.toMap
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      partitions.map { case topicPartition =>
        (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.GROUP_LOAD_IN_PROGRESS.code))}.toMap
    } else {
      // return offsets blindly regardless the current group state since the group may be using
      // Kafka commit storage without automatic group management
      // groupManager也就是GroupMetadataManager
      // 前面的都是校验，这里才是获取offset
      groupManager.getOffsets(groupId, partitions)
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading()) Errors.GROUP_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    if (!isActive.get) {
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, GroupCoordinator.EmptyGroup)
    } else if (!isCoordinatorForGroup(groupId)) {
      (Errors.NOT_COORDINATOR_FOR_GROUP, GroupCoordinator.EmptyGroup)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      (Errors.GROUP_LOAD_IN_PROGRESS, GroupCoordinator.EmptyGroup)
    } else {
      val group = groupManager.getGroup(groupId)
      if (group == null) {
        (Errors.NONE, GroupCoordinator.DeadGroup)
      } else {
        group synchronized {
          (Errors.NONE, group.summary)
        }
      }
    }
  }

  private def onGroupUnloaded(group: GroupMetadata) {
    group synchronized {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      group.transitionTo(Dead)

      previousState match {
        case Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingJoinCallback != null) {
              member.awaitingJoinCallback(joinError(member.memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
              member.awaitingJoinCallback = null
            }
          }
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | AwaitingSync =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingSyncCallback != null) {
              member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR_FOR_GROUP.code)
              member.awaitingSyncCallback = null
            }
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  private def onGroupLoaded(group: GroupMetadata) {
    group synchronized {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable))
      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  // 参数offsetTopicPartitionId为partition.partitionId
  def handleGroupImmigration(offsetTopicPartitionId: Int) {
    groupManager.loadGroupsForPartition(offsetTopicPartitionId, onGroupLoaded)
  }

  // 参数offsetTopicPartitionId为partition.partitionId
  def handleGroupEmigration(offsetTopicPartitionId: Int) {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  // 1.设置分配方案到每个Group成员
  // 2.将新的分配结果下发给每个成员
  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
    assert(group.is(AwaitingSync))
    // 将分配结果保存到每个成员元数据的assignment中
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    propagateAssignment(group, Errors.NONE.code)
  }

  // 1.重置Group成员的所有的分配结果为一个空集合
  // 2.将新的分配结果下发给每个成员
  private def resetAndPropagateAssignmentError(group: GroupMetadata, errorCode: Short) {
    assert(group.is(AwaitingSync))
    group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])
    // 下发分配结果
    propagateAssignment(group, errorCode)
  }
  // 下发分配结果，执行SyncCallbakc，然后重置为null
  private def propagateAssignment(group: GroupMetadata, errorCode: Short) {
    for (member <- group.allMemberMetadata) {
      if (member.awaitingSyncCallback != null) {
        member.awaitingSyncCallback(member.assignment, errorCode)
        member.awaitingSyncCallback = null

        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        // 为给定的Group每一个成员重置DelayedHeartbeat定时任务
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  private def validGroupId(groupId: String): Boolean = {
    groupId != null && !groupId.isEmpty
  }

  private def joinError(memberId: String, errorCode: Short): JoinGroupResult = {
    JoinGroupResult(
      members=Map.empty,
      memberId=memberId,
      generationId=0,
      subProtocol=GroupCoordinator.NoProtocol,
      leaderId=GroupCoordinator.NoLeader,
      errorCode=errorCode)
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  // 为给定的Group每一个成员重置DelayedHeartbeat定时任务
  // 用来检测成员的心跳
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    // complete current heartbeat expectation
    member.latestHeartbeat = time.milliseconds()
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  // 将成员加入组并执行Rebalance
  private def addMemberAndRebalance(sessionTimeoutMs: Int,
                                    clientId: String,
                                    clientHost: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback) = {
    // use the client-id with a random id suffix as the member-id
    // 生成memeberId，等待返回给kafkaConsumer
    val memberId = clientId + "-" + group.generateMemberIdSuffix
    // 生成成员的元数据
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, sessionTimeoutMs, protocols)
    // 保存成员的JoinGroup请求的回调
    member.awaitingJoinCallback = callback
    // 将成员加入group
    group.add(member.memberId, member)
    // 准备Rebalance
    maybePrepareRebalance(group)
    // 返回成员元数据
    member
  }

  // 更新组成员并执行Rebalance
  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    member.supportedProtocols = protocols
    member.awaitingJoinCallback = callback
    // 准备Rebalance
    maybePrepareRebalance(group)
  }

  // 尝试执行Rebalance
  private def maybePrepareRebalance(group: GroupMetadata) {
    group synchronized {
      // 只有Group的状态为Stable或AwaitingSync才可以Rebalance
      if (group.canRebalance)
        // 执行Rebalance
        prepareRebalance(group)
    }
  }

  // 执行Rebalance
  private def prepareRebalance(group: GroupMetadata) {
    // if any members are awaiting sync, cancel their request and have them rejoin
    // 如果当前Group的状态为AwaitingSync，那么返回一个错误响应给kafkaConsumer让其重新发送rejoin
    if (group.is(AwaitingSync))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS.code)
    // 修改group状态为PreparingRebalance
    group.transitionTo(PreparingRebalance)
    info("Preparing to restabilize group %s with old generation %s".format(group.groupId, group.generationId))
    // Rebalance超时时间
    val rebalanceTimeout = group.rebalanceTimeout
    // 封装一个延迟任务，在任务执行前，等待其他Group成员的加入，等到时间后执行流程=> tryCompleteJoin() --> onCompleteJoin()
    val delayedRebalance = new DelayedJoin(this, group, rebalanceTimeout)
    val groupKey = GroupKey(group.groupId)
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  // Group成员故障，包括指定时间内没有收到心跳，或者LeaveGroup
  private def onMemberFailure(group: GroupMetadata, member: MemberMetadata) {
    trace("Member %s in group %s has failed".format(member.memberId, group.groupId))
    group.remove(member.memberId)
    group.currentState match {
      case Dead =>
      case Stable | AwaitingSync => maybePrepareRebalance(group)
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  // 执行JoinGroup的Rebalance
  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group synchronized {
      if (group.notYetRejoinedMembers.isEmpty)
        forceComplete()
      else false
    }
  }

  def onExpireJoin() {
    // TODO: add metrics for restabilize timeouts
  }

  // 正在执行Rebalance的方法
  def onCompleteJoin(group: GroupMetadata) {
    group synchronized {
      val failedMembers = group.notYetRejoinedMembers
      // 移除加入失败的成员
      if (group.isEmpty || !failedMembers.isEmpty) {
        failedMembers.foreach { failedMember =>
          group.remove(failedMember.memberId)
          // TODO: cut the socket connection to the client
        }

        // TODO KAFKA-2720: only remove group in the background thread
        // 如果组为空，设置Group的状态为Dead，然后移除这个无效的Group
        if (group.isEmpty) {
          group.transitionTo(Dead)
          groupManager.removeGroup(group)
          info("Group %s generation %s is dead and removed".format(group.groupId, group.generationId))
        }
      }
      if (!group.is(Dead)) {
        // 获取Group的下一个“代”并修改其状态为AwaitingSync
        group.initNextGeneration()
        info("Stabilized group %s generation %s".format(group.groupId, group.generationId))

        // trigger the awaiting join group response callback for all the members after rebalancing
        // 重新平衡后，为所有成员响应并执行对应的回调
        for (member <- group.allMemberMetadata) {
          assert(member.awaitingJoinCallback != null)
          val joinResult = JoinGroupResult(
            // 如果是Groupleader节点，则返回Group中所有的成员和成员对应的元数据，否则返回空
            members=if (member.memberId == group.leaderId) { group.currentMemberMetadata } else { Map.empty },
            // 返回成员ID
            memberId=member.memberId,
            // 返回"代"
            generationId=group.generationId,
            subProtocol=group.protocol,
            // Group的leader节点ID
            leaderId=group.leaderId,
            // 没有异常
            errorCode=Errors.NONE.code)
          // 执行回调
          member.awaitingJoinCallback(joinResult)
          member.awaitingJoinCallback = null
          // 更新Group成员的心跳定时任务，因为在处理完GROUP_COORDINATOR请求后，kafkaConsuemr启动心跳后台线程任务定时发送心跳
          // 所以当收到JOIN_GROUP请求时也当做了一次心跳，收益这是需要重置心跳检测任务
          // 到时后如果没有收到心跳请求，那么就会认为Group成员发送了故障，需要等待后续的操作
          completeAndScheduleNextHeartbeatExpiration(group, member)
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group synchronized {
      if (shouldKeepMemberAlive(member, heartbeatDeadline) || member.isLeaving)
        forceComplete()
      else false
    }
  }

  def onExpireHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long) {
    group synchronized {
      if (!shouldKeepMemberAlive(member, heartbeatDeadline))
        onMemberFailure(group, member)
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  private def shouldKeepMemberAlive(member: MemberMetadata, heartbeatDeadline: Long) =
    member.awaitingJoinCallback != null ||
      member.awaitingSyncCallback != null ||
      member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadingInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}


object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            time: Time): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkUtils, replicaManager, heartbeatPurgatory, joinPurgatory, time)
  }

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time): GroupCoordinator = {
    val offsetConfig = OffsetConfig(maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, offsetConfig, replicaManager, zkUtils, time)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time)
  }

}
