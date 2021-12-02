import Crypto from 'crypto'
import FSExtra from 'fs-extra'
import Luxon from 'luxon'
import Axios from 'axios'
import AxiosRetry from 'axios-retry'
import AxiosRateLimit from 'axios-rate-limit'
import Ent from 'ent'
import Scramjet from 'scramjet'

function fetcher() {
    const instance = Axios.create({ timeout: 30 * 1000 })
    AxiosRetry(instance, {
        retries: 100,
        shouldResetTimeout: true,
        retryCondition: e => {
            return !e.response || e.response.status === 403 || e.response.status >= 500 || e.response.data.awards?.length === 0
        },
        retryDelay: (number, e) => {
            const url = e.config.url + (e.config.data ? ' ' + e.config.data.replace(/"/g, '') : '')
            if (number === 1) console.log(`Received code ${e.code || e.response?.status}: ${url} (retrying...)`)
            else console.log(`Received code ${e.code || e.response?.status}: ${url} (retry ${number}...)`)
            return 5 * 1000
        }
    })
    AxiosRateLimit(instance, {
        maxRequests: 5, // per second
        perMilliseconds: 1 * 1000
    })
    return async location => {
        const url = typeof location === 'object' ? location.url : location
        const query = location.data ? ' ' + JSON.stringify(location.data).replace(/"/g, '') : ''
        await FSExtra.ensureDir('.scrape-cache')
        const hash = Crypto.createHash('md5').update(JSON.stringify(location)).digest('hex')
        const exists = await FSExtra.pathExists(`.scrape-cache/${hash}`)
        if (exists) {
            console.log(`Using cache (${hash}) for ${url}${query}...`)
            return FSExtra.readJson(`.scrape-cache/${hash}`)
        }
        console.log(`Fetching ${url}${query}...`)
        const response = await instance(location)
        const output = {
            data: response.data
        }
        await FSExtra.writeJson(`.scrape-cache/${hash}`, output)
        return output
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
    const cleanAddress = address => {
        return Ent.decode(address)
            .trim()
            .replace(/(\t+| ?\r?\n ?)/g, ', ') // tabs and newlines become commas plus spaces
            .replace(/ +/g, ' ') // collapse multiple spaces
            .replace(/ +,/g, ',') // spaces preceding commas get removed
            .replace(/,+/g, ',') // remove repeated commas
            .replace(/,(?=[^ ])/g, ', ') // ensure commas are followed by a space
            .replace(/[\.;], /g, ', ') // ensure commas aren't preceded by a full-stop or a semicolon
            .replace(/"/g, '') // remove all quotes
            .replace(/^, /, '') // remove commas at the start
            .replace(/[\.,]$/g, '') // remove full stops or commas at the end
            .trim()
    }
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
            awardSupplierName: award.supplierName ? Ent.decode(award.supplierName) : null,
            awardSupplierCompanyDunsNumber: award.dunsNumber?.match(/^0+$/) ? null : award.dunsNumber,
            awardSupplierAddress: award.supplierAddress ? cleanAddress(award.supplierAddress) : null,
            awardProcedureType: award.awardedProcedureType
        }
    })
}

async function run() {
    const fetch = fetcher()
    Scramjet.DataStream.from(dates('2015-01-01'))
        .map(fetch)
        .flatMap(detail)
        .map(fetch)
        .flatMap(awards)
        .CSVStringify()
        .pipe(FSExtra.createWriteStream('contracts-finder.csv'))
}

run()
