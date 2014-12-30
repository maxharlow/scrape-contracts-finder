import scala.xml.XML
import scala.collection.immutable.ListMap
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.Duration
import dispatch.{Http, enrichFuture, url, as}
import com.github.tototoshi.csv.CSVWriter

object Contracts extends App {

  implicit val context = ExecutionContext.global

  val http = Http.configure(_ setFollowRedirects true)

  val headersAwards = List(
    "noticeId",
    "noticePublishedDate",
    "noticeGroup",
    "noticeForm",
    "noticeSystemState",
    "noticeSystemStateChangeDate",
    "noticeDocsNumber",
    "contractTitle",
    "contractDescription",
    "contractLocation",
    "contractClassifications",
    "contractAwardDate",
    "contractAwardValue",
    "contractAwardReference",
    "buyerReference",
    "buyerGroupId",
    "buyerGroupName",
    "buyerOrgName",
    "buyerContact",
    "supplierName",
    "supplierCompanyNumber",
    "supplierContact")

  val headersTenders = List(
    "noticeId",
    "noticePublishedDate",
    "noticeState",
    "noticeGroup",
    "noticeForm",
    "noticeSystemState",
    "noticeSystemStateChangeDate",
    "noticeDocsNumber",
    "contractTitle",
    "contractDescription",
    "contractLocation",
    "contractClassifications",
    "contractDeadlineDate",
    "contractDeadlineDateFor",
    "contractValueFrom",
    "contractValueTo",
    "buyerReference",
    "buyerGroupId",
    "buyerGroupName",
    "buyerOrgName",
    "buyerContact"
  )

  run()

  def run() {
    val csvAwards = CSVWriter.open("contracts-awards.csv")
    val csvTenders = CSVWriter.open("contracts-tenders.csv")
    csvAwards.writeRow(headersAwards)
    csvTenders.writeRow(headersTenders)

    def writeAward(record: Map[String, String]): Unit = csvAwards.writeRow(record.values.toSeq)
    def writeTender(record: Map[String, String]): Unit = csvTenders.writeRow(record.values.toSeq)

    for (year <- 2011 to 2014; month <- 1 to 12) {
      load(retrieve(locate(year, month))) foreach {
        case notice if notice.label == "CONTRACT_AWARD" => writeAward(selectAward(notice))
        case notice if notice.label == "CONTRACT" => writeTender(selectTender(notice))
        case notice if notice.label == "PRIOR_INFORMATION" => // ignore for now
      }
    }

    csvAwards.close()
    csvTenders.close()
  }

  def locate(year: Int, month: Int): String = {
    val monthFormatted = "%02d".format(month)
    return s"http://www.contractsfinder.businesslink.gov.uk/public_files/Notices/Monthly/notices_${year}_${monthFormatted}.xml"
  }

  def retrieve(location: String): String = {
    http(url(location) OK as.String).apply()
  }

  def load(data: String): Seq[xml.Node] = {
    if (data == "Error processing the request") Nil
    else for {
      xml <- XML.loadString(data.dropWhile(_ != '<'))
      notices <- xml \ "NOTICES" \ "_"
    }
    yield notices
  }

  def selectAward(award: xml.Node): ListMap[String, String] = {
    ListMap(
      "noticeId" -> (award \ "SYSTEM" \ "NOTICE_ID").text,
      "noticePublishedDate" -> (award \ "SYSTEM" \ "SYSTEM_PUBLISHED_DATE").text,
      "noticeGroup" -> (award \ "SYSTEM" \ "NOTICE_TYPE_FRIENDLY_NAME").text,
      "noticeForm" -> {
        val formNumber = (award \ "@FORM").text.toInt
        if (formNumber == 1 || formNumber == 2 || formNumber == 3) "Below-OJEU"
        else if (formNumber == 90 || formNumber == 91) "Transparency"
        else if (formNumber == 92 || formNumber == 93) "Subcontract" // never seems to occur?
        else "UNKNOWN"
      },
      "noticeSystemState" -> (award \ "SYSTEM" \ "SYSTEM_NOTICE_STATE").text,
      "noticeSystemStateChangeDate" -> (award \ "SYSTEM" \ "SYSTEM_NOTICE_STATE_CHANGE_DATE").text,
      "noticeDocsNumber" -> (award \ "FD_CONTRACT_AWARD" \ "DOCUMENTS" \ "_").length.toString,
      "contractTitle" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "TITLE_CONTRACT").text.trim,
      "contractDescription" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "SHORT_CONTRACT_DESCRIPTION").text.trim,
      "contractLocation" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "LOCATION_NUTS" \\ "p").head.text,
      "contractClassifications" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "CPV" \\ "CPV_CODE").map(_.\("@CODE").text).mkString(";"),
      "contractAwardDate" -> {
        val datesValues = award \ "FD_CONTRACT_AWARD" \ "AWARD_OF_CONTRACT" \ "CONTRACT_AWARD_DATE"
        val dates = datesValues map { dateValue =>
          if (dateValue.isEmpty) "" else (dateValue \ "YEAR").text + "-" + (dateValue \ "MONTH").text + "-" + (dateValue \ "DAY").text
        }
        dates.mkString(";")
      },
      "contractAwardValue" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "TOTAL_FINAL_VALUE" \ "COSTS_RANGE_AND_CURRENCY_WITH_VAT_RATE" \ "VALUE_COST").text.replaceAll(" ", ""),
      "buyerReference" -> (award \ "FD_CONTRACT_AWARD" \ "PROCEDURE_DEFINITION_CONTRACT_AWARD_NOTICE" \ "ADMINISTRATIVE_INFORMATION_CONTRACT_AWARD" \ "FILE_REFERENCE_NUMBER").text,
      "buyerGroupId" -> (award \ "SYSTEM" \ "BUYER_GROUP_ID").text,
      "buyerGroupName" -> (award \ "SYSTEM" \ "BUYER_GROUP_NAME").text.trim,
      "buyerOrgName" -> (award \ "FD_CONTRACT_AWARD" \ "CONTRACTING_AUTHORITY_INFORMATION" \ "NAME_ADDRESSES_CONTACT_CONTRACT_AWARD" \ "CA_CE_CONCESSIONAIRE_PROFILE" \ "ORGANISATION").text.trim,
      "buyerContact" -> (award \ "FD_CONTRACT_AWARD" \ "CONTRACTING_AUTHORITY_INFORMATION" \ "NAME_ADDRESSES_CONTACT_CONTRACT_AWARD" \ "CA_CE_CONCESSIONAIRE_PROFILE" \ "E_MAIL").text.trim,
      "supplierName" -> (award \ "FD_CONTRACT_AWARD" \ "AWARD_OF_CONTRACT" \ "ECONOMIC_OPERATOR_NAME_ADDRESS" \ "CONTACT_DATA_WITHOUT_RESPONSIBLE_NAME" \ "ORGANISATION").map(_.text.trim).mkString(";"),
      "supplierCompanyNumber" -> (award \ "FD_CONTRACT_AWARD" \ "AWARD_OF_CONTRACT" \ "COMPANIES_HOUSE_URI_SUFFIX").map(_.text).mkString(";"),
      "supplierContact" -> (award \ "FD_CONTRACT_AWARD" \ "AWARD_OF_CONTRACT" \ "ECONOMIC_OPERATOR_NAME_ADDRESS" \ "EMAIL").map(_.text.trim).mkString(";")
    )
  }

  def selectTender(tender: xml.Node): ListMap[String, String] = {
    ListMap(
      "noticeId" -> (tender \ "SYSTEM" \ "NOTICE_ID").text,
      "noticePublishedDate" -> (tender \ "SYSTEM" \ "SYSTEM_PUBLISHED_DATE").text,
      "noticeState" -> (tender \ "SYSTEM" \ "NOTICE_STATE").text,
      "noticeGroup" -> (tender \ "SYSTEM" \ "NOTICE_TYPE_FRIENDLY_NAME").text,
      "noticeForm" -> {
        val formNumber = (tender \ "@FORM").text.toInt
        if (formNumber == 1 || formNumber == 2 || formNumber == 3) "Below-OJEU"
        else if (formNumber == 90 || formNumber == 91) "Transparency"
        else if (formNumber == 92 || formNumber == 93) "Subcontract" // never seems to occur?
        else "UNKNOWN"
      },
      "noticeSystemState" -> (tender \ "SYSTEM" \ "SYSTEM_NOTICE_STATE").text,
      "noticeSystemStateChangeDate" -> (tender \ "SYSTEM" \ "SYSTEM_NOTICE_STATE_CHANGE_DATE").text,
      "noticeDocsNumber" -> (tender \ "FD_CONTRACT" \ "DOCUMENTS" \ "_").length.toString,
      "contractTitle" -> (tender \ "FD_CONTRACT" \ "OBJECT_CONTRACT_INFORMATION" \ "DESCRIPTION_CONTRACT_INFORMATION" \ "TITLE_CONTRACT").text.trim,
      "contractDescription" -> (tender \ "FD_CONTRACT" \ "OBJECT_CONTRACT_INFORMATION" \ "DESCRIPTION_CONTRACT_INFORMATION" \ "SHORT_CONTRACT_DESCRIPTION").text.trim,
      "contractLocation" -> (tender \ "FD_CONTRACT" \ "OBJECT_CONTRACT_INFORMATION" \ "DESCRIPTION_CONTRACT_INFORMATION" \ "LOCATION_NUTS" \\ "p").head.text,
      "contractClassifications" -> (tender \ "FD_CONTRACT" \ "OBJECT_CONTRACT_INFORMATION" \ "DESCRIPTION_CONTRACT_INFORMATION" \ "CPV" \\ "CPV_CODE").map(_.\("@CODE").text).mkString(";"),
      "contractDeadlineDate" -> {
        val dateValue = tender \ "FD_CONTRACT" \ "PROCEDURE_DEFINITION_CONTRACT_NOTICE" \ "ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE" \ "RECEIPT_LIMIT_DATE"
        if ((dateValue \ "YEAR").text.isEmpty) "" else (dateValue \ "YEAR").text + "-" + (dateValue \ "MONTH").text + "-" + (dateValue \ "DAY").text
      },
      "contractDeadlineDateFor" -> (tender \ "FD_CONTRACT" \ "PROCEDURE_DEFINITION_CONTRACT_NOTICE" \ "ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE" \ "RECEIPT_LIMIT_DATE" \ "RECEIPT_LIMIT_DATE_FOR").text,
      "contractValueFrom" -> (tender \ "FD_CONTRACT" \ "OBJECT_CONTRACT_INFORMATION" \ "QUANTITY_SCOPE" \ "NATURE_QUANTITY_SCOPE" \ "COSTS_RANGE_AND_CURRENCY" \ "RANGE_VALUE_COST" \ "LOW_VALUE").text,
      "contractValueTo" -> (tender \ "FD_CONTRACT" \ "OBJECT_CONTRACT_INFORMATION" \ "QUANTITY_SCOPE" \ "NATURE_QUANTITY_SCOPE" \ "COSTS_RANGE_AND_CURRENCY" \ "RANGE_VALUE_COST" \ "HIGH_VALUE").text,
      "buyerReference" -> (tender \ "FD_CONTRACT" \ "PROCEDURE_DEFINITION_CONTRACT_NOTICE" \ "ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE" \ "FILE_REFERENCE_NUMBER").text,
      "buyerGroupId" -> (tender \ "SYSTEM" \ "BUYER_GROUP_ID").text,
      "buyerGroupName" -> (tender \ "SYSTEM" \ "BUYER_GROUP_NAME").text.trim,
      "buyerOrgName" -> (tender \ "FD_CONTRACT" \ "CONTRACTING_AUTHORITY_INFORMATION" \ "NAME_ADDRESSES_CONTACT_CONTRACT" \ "CA_CE_CONCESSIONAIRE_PROFILE" \ "ORGANISATION").text.trim,
      "buyerContact" -> (tender \ "FD_CONTRACT" \ "CONTRACTING_AUTHORITY_INFORMATION" \ "NAME_ADDRESSES_CONTACT_CONTRACT" \ "CA_CE_CONCESSIONAIRE_PROFILE" \ "E_MAIL").text.trim
    )
  }

}
