package org.encalmo.aws

import software.amazon.awssdk.services.iam.model.*

import ujson.{Arr, Str}
import upickle.default.*
import upickle.default.ReadWriter.join

import scala.jdk.CollectionConverters.*

object AwsIamApi {

  inline def getRole(roleName: String)(using AwsClient): Role = {
    summon[AwsClient].iam
      .getRole(GetRoleRequest.builder().roleName(roleName).build())
      .role()
  }

  inline def listRoleAttachedPolicies(
      roleName: String
  )(using AwsClient): Seq[AttachedPolicy] = {
    summon[AwsClient].iam
      .listAttachedRolePolicies(
        ListAttachedRolePoliciesRequest
          .builder()
          .roleName(roleName)
          .build()
      )
      .attachedPolicies()
      .asScala
      .toSeq
  }

  inline def listRoleInlinedPolicies(
      roleName: String
  )(using AwsClient): Seq[String] = {
    summon[AwsClient].iam
      .listRolePolicies(
        ListRolePoliciesRequest
          .builder()
          .roleName(roleName)
          .build()
      )
      .policyNames()
      .asScala
      .toSeq
  }

  inline def getRolePolicy(roleName: String, policyName: String)(using
      AwsClient
  ): String = {
    summon[AwsClient].iam
      .getRolePolicy(
        GetRolePolicyRequest
          .builder()
          .roleName(roleName)
          .policyName(policyName)
          .build()
      )
      .policyDocument()
  }

  inline def getPolicy(policyArn: String)(using AwsClient): Policy = {
    summon[AwsClient].iam
      .getPolicy(
        GetPolicyRequest
          .builder()
          .policyArn(policyArn: String)
          .build()
      )
      .policy()
  }

  inline def getPolicyVersionDocument(policyArn: String, versionId: String)(using
      AwsClient
  ): String = {
    summon[AwsClient].iam
      .getPolicyVersion(
        GetPolicyVersionRequest
          .builder()
          .policyArn(policyArn: String)
          .versionId(versionId: String)
          .build()
      )
      .policyVersion()
      .document()
  }

  implicit val stringOrStringArrayReadWrite: ReadWriter[Seq[String]] =
    readwriter[ujson.Value].bimap(
      { case ss: Seq[String] =>
        if (ss.isEmpty) then ujson.Null
        else if (ss.size == 1) then ujson.Str(ss.head)
        else ujson.Arr(ss.map(ujson.Str(_)))
      },
      {
        case ujson.Str(s) => Seq(s)
        case Arr(a) =>
          a.map {
            case Str(s) => s
            case other =>
              throw new Exception(
                s"Expected string item in the arrays but got $other"
              )
          }.toSeq
        case other =>
          throw new Exception(
            s"Expected string or array of strings but got $other"
          )
      }
    )

  case class PolicyStatement(
      Effect: String,
      Action: Seq[String],
      Resource: Seq[String]
  ) derives ReadWriter

}
