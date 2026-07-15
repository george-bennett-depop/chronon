package ai.chronon.online.fetcher

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.serde.AvroConversions
import org.junit.Assert.{assertEquals, assertNotEquals}
import org.scalatest.flatspec.AnyFlatSpec

class JoinRequestKeysTest extends AnyFlatSpec {

  private val groupBy = Builders.GroupBy(
    metaData = Builders.MetaData(name = "unit_test.query_group_by"),
    keyColumns = Seq("query_normalized")
  )

  private def join(selectExpr: String, setups: Seq[String] = Seq.empty): Join =
    Builders.Join(
      metaData = Builders.MetaData(name = "unit_test.query_join"),
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("query_normalized" -> selectExpr),
          setups = setups
        ),
        table = "unit_test.queries"
      ),
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = groupBy,
          keyMapping = Map("query_normalized" -> "query_normalized")
        ))
    )

  private def servingInfo(inputSchema: StructType): GroupByServingInfoParsed =
    servingInfo(groupBy, StructType("Key", Array(StructField("query_normalized", StringType))), inputSchema)

  private def servingInfo(groupBy: GroupBy,
                          keySchema: StructType,
                          inputSchema: StructType): GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(groupBy)
    groupByServingInfo.setKeyAvroSchema(AvroConversions.fromChrononSchema(keySchema).toString)
    groupByServingInfo.setInputAvroSchema(AvroConversions.fromChrononSchema(inputSchema).toString)
    new GroupByServingInfoParsed(groupByServingInfo)
  }

  it should "include join left selects in cached key mapping keys" in {
    val firstJoin = join("lower(query)")
    val secondJoin = join("trim(lower(query))")

    assertNotEquals(
      JoinRequestKeys.partKey(firstJoin, firstJoin.joinPartOps.head),
      JoinRequestKeys.partKey(secondJoin, secondJoin.joinPartOps.head)
    )
  }

  it should "include join left setups in part keys" in {
    val firstJoin = join("normalize(query)", Seq("CREATE TEMPORARY FUNCTION normalize AS 'first.Normalize'"))
    val secondJoin = join("normalize(query)", Seq("CREATE TEMPORARY FUNCTION normalize AS 'second.Normalize'"))

    assertNotEquals(
      JoinRequestKeys.partKey(firstJoin, firstJoin.joinPartOps.head),
      JoinRequestKeys.partKey(secondJoin, secondJoin.joinPartOps.head)
    )
  }

  it should "include GroupBy serving schema in schema-qualified part keys" in {
    val queryJoin = join("lower(query)")
    val firstServingInfo = servingInfo(StructType("Input", Array(StructField("query", StringType))))
    val secondServingInfo =
      servingInfo(StructType("Input", Array(StructField("query", StringType), StructField("locale", StringType))))

    assertNotEquals(
      JoinRequestKeys.partKey(queryJoin, queryJoin.joinPartOps.head, firstServingInfo),
      JoinRequestKeys.partKey(queryJoin, queryJoin.joinPartOps.head, secondServingInfo)
    )
  }

  it should "keep part keys stable for equivalent select ordering" in {
    def orderedJoin(selects: Map[String, String]): Join =
      Builders.Join(
        left = Builders.Source.events(
          query = Builders.Query(selects = selects),
          table = "unit_test.queries"
        ),
        joinParts = Seq(
          Builders.JoinPart(
            groupBy = groupBy,
            keyMapping = Map("query_normalized" -> "query_normalized")
          ))
      )

    val firstJoin = orderedJoin(Map("query_normalized" -> "lower(query)", "unused" -> "unused"))
    val secondJoin = orderedJoin(Map("unused" -> "unused", "query_normalized" -> "lower(query)"))

    assertEquals(
      JoinRequestKeys.partKey(firstJoin, firstJoin.joinPartOps.head),
      JoinRequestKeys.partKey(secondJoin, secondJoin.joinPartOps.head)
    )
  }

  it should "validate missing request keys against raw selected inputs" in {
    val queryJoin = join("lower(query)")
    val joinPart = queryJoin.joinPartOps.head

    assertEquals(
      Seq("query"),
      JoinRequestKeys.missingRequestKeys(Request(queryJoin.metaData.name, Map.empty), queryJoin, joinPart)
    )
    assertEquals(
      Seq.empty,
      JoinRequestKeys.missingRequestKeys(Request(queryJoin.metaData.name, Map("query" -> "SHOES")), queryJoin, joinPart)
    )
    assertEquals(
      Seq.empty,
      JoinRequestKeys.missingRequestKeys(
        Request(queryJoin.metaData.name, Map("query_normalized" -> "shoes")),
        queryJoin,
        joinPart)
    )
  }

  it should "derive GroupBy keys from struct-extracting select expressions" in {
    val testGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "unit_test.test_group_by"),
      keyColumns = Seq("test_id")
    )
    val testJoin = Builders.Join(
      metaData = Builders.MetaData(name = "unit_test.test_join"),
      left = Builders.Source.events(
        query = Builders.Query(selects = Map("test_id" -> "CAST(data.testId AS BIGINT)")),
        table = "unit_test.test_events"
      ),
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = testGroupBy,
          keyMapping = Map("testId" -> "test_id")
        ))
    )
    val joinPart = testJoin.joinPartOps.head

    val keyServingInfo = servingInfo(
      testGroupBy,
      StructType("Key", Array(StructField("test_id", LongType))),
      StructType("Input", Array(StructField("test_id", LongType)))
    )
    val request = Request(testJoin.metaData.name, Map("data" -> Map("testId" -> 123L)))

    assertEquals(
      Map("test_id" -> 123L),
      JoinRequestKeys.deriveLeftKeys(request, testJoin, joinPart, keyServingInfo)
    )

    // Wrong-shaped value: scalar where the inferred schema expects a struct.
    val badRequest = Request(testJoin.metaData.name, Map("data" -> "not-a-struct"))
    val thrown = intercept[IllegalArgumentException] {
      JoinRequestKeys.deriveLeftKeys(badRequest, testJoin, joinPart, keyServingInfo)
    }
    assert(thrown.getMessage.contains("test_id=CAST(data.testId AS BIGINT)"))
    assert(thrown.getMessage.contains("data: expected"))
    assert(thrown.getMessage.contains("got java.lang.String"))
  }

  it should "derive GroupBy keys from raw request keys" in {
    val queryJoin = join("lower(query)")
    val joinPart = queryJoin.joinPartOps.head
    val request = Request(queryJoin.metaData.name, Map("query" -> "SHOES"))
    val keyServingInfo = servingInfo(StructType("Input", Array(StructField("query", StringType))))

    assertEquals(true, JoinRequestKeys.needsDerivation(request, queryJoin, joinPart))
    assertEquals(
      Map("query_normalized" -> "shoes"),
      JoinRequestKeys.deriveLeftKeys(request, queryJoin, joinPart, keyServingInfo)
    )
  }
}
