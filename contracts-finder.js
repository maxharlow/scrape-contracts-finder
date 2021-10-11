import FSExtra from 'fs-extra'
import Luxon from 'luxon'
import Axios from 'axios'
import AxiosRetry from 'axios-retry'
import Ent from 'ent'
import Scramjet from 'scramjet'

async function fetch(location) {
    const url = typeof location === 'object' ? location.url : location
    const instance = Axios.create({ timeout: 30 * 1000 })
    AxiosRetry(instance, {
        retries: 10,
        shouldResetTimeout: true,
        retryCondition: e => {
            return !e.response || e.response.status >= 500 || e.response.data.awards?.length === 0
        },
        retryDelay: (number, e) => {
            if (number === 1) console.log(`Received code ${e.code || e.response?.status}: ${url} (retrying...)`)
            else console.log(`Received code ${e.code || e.response?.status}: ${url} (retry ${number}...)`)
            return 5 * 1000
        }
    })
    const response = await instance(location)
    return {
        data: response.data
    }
}

function dates(from, a = []) {
    const day = Luxon.DateTime.fromFormat(from, 'yyyy-MM-dd')
    if (day >= Luxon.DateTime.now()) return a
    const date = day.toFormat('yyyy-MM-dd')
    const datePlusOne = day.plus({ days: 1 }).toFormat('yyyy-MM-dd')
    const request = {
        method: 'POST',
        url: 'https://www.contractsfinder.service.gov.uk/api/rest/2/search_notices/json',
        headers: {
            'content-type': 'application/json'
        },
        data: {
            searchCriteria: {
                statuses: 'Awarded',
                publishedFrom: date,
                publishedTo: date
            },
            size: 1000
        }
    }
    return dates(datePlusOne, a.concat(request))
}

function detail(response) {
    return response.data.noticeList.map(notice => {
        return `https://www.contractsfinder.service.gov.uk/api/rest/2/get_published_notice/json/${notice.item.id}`
    })
}

function awards(response) {
    const notice = response.data.notice
    return response.data.awards.map(award => {
        return {
            noticeURL: `https://www.contractsfinder.service.gov.uk/notice/${notice.id}`,
            noticeFrom: Ent.decode(notice.organisationName),
            noticeTitle: Ent.decode(notice.title),
            noticeDescription: Ent.decode(notice.description).replace(/\r?\n+/g, ' ').replace(/\s+/g, ' '),
            noticeType: notice.type,
            noticeStatus: notice.status,
            noticeValueLow: notice.valueLow,
            noticeValueHigh: notice.valueHigh,
            noticeProcedureType: notice.procedureType,
            noticeIsFrameworkAgreement: notice.isFrameworkAgreement,
            noticeDeadlineDate: notice.deadlineDate,
            noticePublishedDate: notice.publishedDate,
            noticeCPVs: notice.cpvCodes.join('; '),
            awardID: award.id,
            awardValue: award.value,
            awardSupplierValue: award.supplierAwardedValue, // what is this?
            awardSupplierName: Ent.decode(award.supplierName),
            awardSupplierCompanyDunsNumber: award.dunsNumber?.match(/^0+$/) ? null : award.dunsNumber,
            awardSupplierAddress: Ent.decode(award.supplierAddress).replace(/ ?\r?\n+/g, ', ').replace(/\s+/g, ' ').replace(/,,+/g, ','),
            awardProcedureType: award.awardedProcedureType
        }
    })
}

async function run() {
    Scramjet.DataStream.from(dates('2018-01-01'))
        .map(fetch)
        .flatMap(detail)
        .map(fetch)
        .flatMap(awards)
        .CSVStringify()
        .pipe(FSExtra.createWriteStream('contracts-finder.csv'))
}

run()
