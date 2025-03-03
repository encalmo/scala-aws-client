package org.encalmo.aws

import software.amazon.awssdk.services.sqs.model.*

import scala.jdk.CollectionConverters.*

object AwsSqsApi {

  /** Returns a list of your queue URLs in the current region. */
  inline def listQueues()(using AwsClient): Seq[String] =
    AwsClient.invoke(s"listQueues") {
      summon[AwsClient].sqs
        .listQueues(ListQueuesRequest.builder().build())
        .queueUrls()
        .asScala
        .toSeq
    }

  /** Gets attributes for the specified queue. */
  inline def getQueueAttributes(
      queueUrl: String
  )(using AwsClient): Map[String, String] =
    AwsClient.invoke(s"getQueueAttributes $queueUrl") {
      summon[AwsClient].sqs
        .getQueueAttributes(
          GetQueueAttributesRequest.builder().queueUrl(queueUrl).build()
        )
        .attributesAsStrings()
        .asScala
        .toMap
    }

  /** Returns a list of your queues that have the RedrivePolicy queue attribute configured with a dead-letter queue.
    */
  inline def listDeadLetterSourceQueues(
      queueUrl: String
  )(using AwsClient): Seq[String] =
    AwsClient.invoke(s"listDeadLetterSourceQueues $queueUrl") {
      summon[AwsClient].sqs
        .listDeadLetterSourceQueues(
          ListDeadLetterSourceQueuesRequest.builder().queueUrl(queueUrl).build()
        )
        .queueUrls()
        .asScala
        .toSeq
    }

  /** List all cost allocation tags added to the specified Amazon SQS queue. */
  inline def listQueueTags(
      queueUrl: String
  )(using AwsClient): Seq[(String, String)] =
    AwsClient.invoke(s"listQueueTags $queueUrl") {
      summon[AwsClient].sqs
        .listQueueTags(
          ListQueueTagsRequest.builder().queueUrl(queueUrl).build()
        )
        .tags()
        .asScala
        .toSeq
    }

  /** Delivers a message to the specified queue.
    *
    * A message can include only XML, JSON, and unformatted text. The following Unicode characters are allowed:
    *
    * #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF
    *
    * Any characters not included in this list will be rejected
    */
  inline def sendMessage(
      queueUrl: String,
      message: String,
      delaySeconds: Option[Integer] = None,
      messageGroupId: Option[String] = None,
      messageDeduplicationId: Option[String] = None,
      messageAttributes: Option[Map[String, MessageAttributeValue]] = None
  )(using AwsClient): SendMessageResponse =
    AwsClient.invoke(s"sendMessage $queueUrl $message") {
      summon[AwsClient].sqs
        .sendMessage(
          SendMessageRequest
            .builder()
            .queueUrl(queueUrl)
            .messageBody(message)
            .optionally[Integer](delaySeconds, _.delaySeconds)
            .optionally(messageGroupId, _.messageGroupId)
            .optionally(messageDeduplicationId, _.messageDeduplicationId)
            .optionally(messageAttributes.map(_.asJava), _.messageAttributes)
            .build()
        )
    }

  /** You can use SendMessageBatch to send up to 10 messages to the specified queue by assigning either identical or
    * different values to each message (or by not assigning values at all). This is a batch version of [SendMessage]().
    * For a FIFO queue, multiple messages within a single batch are enqueued in the order they are sent.
    *
    * The result of sending each message is reported individually in the response. Because the batch request can result
    * in a combination of successful and unsuccessful actions, you should check for batch errors even when the call
    * returns an HTTP status code of 200.
    *
    * The maximum allowed individual message size and the maximum total payload size (the sum of the individual lengths
    * of all of the batched messages) are both 256 KiB (262,144 bytes).
    *
    * A message can include only XML, JSON, and unformatted text. The following Unicode characters are allowed:
    *
    * #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF
    *
    * Any characters not included in this list will be rejected
    */
  inline def sendMessageBatch(
      queueUrl: String,
      entries: Seq[SendMessageBatchRequestEntry]
  )(using AwsClient): SendMessageBatchResponse =
    AwsClient.invoke(s"sendMessageBatch $queueUrl $entries") {
      summon[AwsClient].sqs
        .sendMessageBatch(
          SendMessageBatchRequest
            .builder()
            .queueUrl(queueUrl)
            .entries(entries.asJava)
            .build()
        )
    }

}
