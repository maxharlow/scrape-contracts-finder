import java.io.File
import scala.xml.XML
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import dispatch._
import dispatch.Defaults._
import com.github.tototoshi.csv.CSVWriter

object Extract extends App {

  val http = Http.configure(_ setFollowRedirects true)

  println("""
    ,---,
    |___|    .        ___ __   _   _  ___  _  __
    /``\ |---^-,_      |  |_) /_\ / `  |  / \ |_)
   ( () )  ( o )||     |  | \ | | \_,  |  \_/ | \
    \__/````\_/  \
  """)

  run()

  def run() {
    val awardsCsv = CSVWriter.open(new File("awards.csv"))
    val headers = List(
      "noticeId",
      "referenceNumber",
      "publishedDate",
      "value",
      "status",
      "orgName",
      "orgContact",
      "title",
      "description",
      "awardeeName",
      "awardeeCompanyNumber",
      "awardDate",
      "noticeType",
      "region",
      "noticeState",
      "noticeStateChangeDate",
      "classification",
      "numDocs")
    awardsCsv.writeRow(headers)
    for {
      year <- 2011 to 2014
      month <- 1 to 12
    }
    yield for (data <- retrieve(year, month))
    yield for (item <- process(data)) {
      val selected = select(item)
      awardsCsv.writeRow(selected.values.toSeq)
    }
    awardsCsv.close()
  }

  def retrieve(year: Int, month: Int): Future[String] = {
    println(s"Now retrieving $year-$month...")
    val monthFormatted = "%02d".format(month)
    val response = http {
      url(s"http://www.contractsfinder.businesslink.gov.uk/public_files/Notices/Monthly/notices_${year}_${monthFormatted}.xml") OK as.String
    }
    Await.ready(response, 1.minute)
    response
  }

  def process(data: String): Seq[xml.Node] = {
    for {
      xml <- XML.loadString(data.dropWhile(_ != '<'))
      body <- xml \ "NOTICES" \ "_"
      awards <- body.filter(_.label == "CONTRACT_AWARD")
      award <- awards
    }
    yield awards
  }

  def select(award: xml.Node): ListMap[String, String] = {
    ListMap(
      "noticeId" -> (award \ "SYSTEM" \ "NOTICE_ID").text,
      "referenceNumber" -> (award \ "FD_CONTRACT_AWARD" \ "PROCEDURE_DEFINITION_CONTRACT_AWARD_NOTICE" \ "ADMINISTRATIVE_INFORMATION_CONTRACT_AWARD" \ "FILE_REFERENCE_NUMBER").text,
      "publishedDate" -> (award \ "SYSTEM" \ "SYSTEM_PUBLISHED_DATE").text,
      "value" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "TOTAL_FINAL_VALUE" \ "COSTS_RANGE_AND_CURRENCY_WITH_VAT_RATE" \ "VALUE_COST").text,
      "status" -> (award \ "SYSTEM" \ "SYSTEM_NOTICE_STATE").text,
      "orgName" -> (award \ "FD_CONTRACT_AWARD" \ "CONTRACTING_AUTHORITY_INFORMATION" \ "NAME_ADDRESSES_CONTACT_CONTRACT_AWARD" \ "CA_CE_CONCESSIONAIRE_PROFILE" \ "ORGANISATION").text,
      "orgContact" -> (award \ "FD_CONTRACT_AWARD" \ "CONTRACTING_AUTHORITY_INFORMATION" \ "NAME_ADDRESSES_CONTACT_CONTRACT_AWARD" \ "CA_CE_CONCESSIONAIRE_PROFILE" \ "E_MAIL").text,
      "title" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "TITLE_CONTRACT").text,
      "description" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "SHORT_CONTRACT_DESCRIPTION").text,
      "awardeeName" -> (award \ "FD_CONTRACT_AWARD" \ "AWARD_OF_CONTRACT" \ "ECONOMIC_OPERATOR_NAME_ADDRESS" \ "CONTACT_DATA_WITHOUT_RESPONSIBLE_NAME" \ "ORGANISATION").map(_.text).mkString(";"),
      "awardeeCompanyNumber" -> (award \ "FD_CONTRACT_AWARD" \ "AWARD_OF_CONTRACT" \ "COMPANIES_HOUSE_URI_SUFFIX").map(_.text).mkString(";"),
      "awardDate" -> {
        val datesValues = award \ "FD_CONTRACT_AWARD" \ "AWARD_OF_CONTRACT" \ "CONTRACT_AWARD_DATE"
        val dates = datesValues map { dateValue =>
          if (dateValue.isEmpty) "" else (dateValue \ "DAY").text + "/" + (dateValue \ "MONTH").text + "/" + (dateValue \ "YEAR").text
        }
        dates.mkString(";")
      },
      "noticeType" -> (award \ "SYSTEM" \ "NOTICE_TYPE_FRIENDLY_NAME").text,
      "region" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "LOCATION_NUTS" \\ "p").head.text,
      "noticeState" -> (award \ "SYSTEM" \ "NOTICE_STATE").text,
      "noticeStateChangeDate" -> (award \ "SYSTEM" \ "SYSTEM_NOTICE_STATE_CHANGE_DATE").text,
      "classification" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "CPV" \\ "CPV_CODE").map(_ \ "@CODE" text).mkString("|"),
      "numDocs" -> (award \ "FD_CONTRACT_AWARD" \ "DOCUMENTS" \ "_").length.toString
    )
  }

}
