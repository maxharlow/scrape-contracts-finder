const Ix = require('ix')
const Axios = require('axios')
const AxiosRetry = require('axios-retry')
const Ent = require('ent')
const Progress = require('progress')
const PapaParse = require('papaparse')
const FSExtra = require('fs-extra')

async function request(location) {
    const url = typeof location === 'object' ? location.url : location
    const timeout = 30 * 1000
    const instance = Axios.create({ timeout })
    AxiosRetry(instance, {
        retries: 10,
        shouldResetTimeout: true,
        retryCondition: e => {
            return !e.response || e.response.status >= 500 // no response or server error
        },
        retryDelay: (number, e) => {
            if (number === 1) console.log(`Received code ${e.code}: ${e.config.url}`)
            else console.log(`  → Received code ${e.code} in retry attempt #${number - 1}: ${e.config.url}`)
            return 5 * 1000
        }
    })
    const response = await instance(location)
    return {
        url,
        data: response.data,
        passthrough: location.passthrough
    }
}

function paginate(response) {
    const pages = response.data.maxPage
    console.log(`Found ${pages} pages.`)
    return Array.from({ length: pages }).map((_, i) => {
        const page = i + 1
        return {
            url: `https://www.contractsfinder.service.gov.uk/Published/Notices/OCDS/Search?stages=award&page=${page}`,
            passthrough: { page }
        }
    })
}

function detail(response) {
    return response.data.results.flatMap(result => {
        const id = result.uri.split(/\/|\./)[10]
        return result.releases.flatMap(release => {
            if (release.awards === undefined) return []
            return release.awards.flatMap(award => {
                return award.suppliers.flatMap(supplier => {
                    return {
                        contractUrl: `https://www.contractsfinder.service.gov.uk/Notice/${id}`,
                        contractTitle: Ent.decode(release.tender.title).replace(/ *\r?\n */g, ' '),
                        contractDescription: Ent.decode(release.tender.description.replace(/\r\n/g, '\n')),
                        contractValue: release.tender.minValue.amount,
                        contractValueMin: release.tender.minValue.amount,
                        contractPublished: result.publishedDate,
                        buyerName: Ent.decode(release.buyer.name),
                        buyerAddress: [
                            release.buyer.address.streetAddress,
                            release.buyer.address.locality,
                            release.buyer.address.region,
                            release.buyer.address.postalCode,
                            release.buyer.address.countryName
                        ].map(x => Ent.decode((x || '').trim())).filter(x => x).join(', ').replace(/,? *\r?\n/g, ', '),
                        buyerContactName: release.buyer.contactPoint.name,
                        buyerContactEmail: release.buyer.contactPoint.email,
                        buyerContactPhone: release.buyer.contactPoint.telephone,
                        buyerContactUrl: release.buyer.contactPoint.uri,
                        supplierName: Ent.decode(supplier.name),
                        supplierID: supplier.identifier.id,
                        supplierIDScheme: supplier.identifier.scheme,
                        supplierAddress: [
                            supplier.address.streetAddress,
                            supplier.address.locality,
                            supplier.address.region,
                            supplier.address.postalCode,
                            supplier.address.countryName
                        ].map(x => Ent.decode((x || '').trim())).filter(x => x).join(', ').replace(/,? *\r?\n/g, ', '),
                        supplierContactName: supplier.contactPoint.name,
                        awardValue: award.value.amount,
                        awardGiven: award.date,
                        awardStart: award.contractPeriod ? award.contractPeriod.startDate : null,
                        awardEnd: award.contractPeriod ? award.contractPeriod.endDate : null
                    }
                })
            })
        })
    })
}

function ticker(text, total) {
    const progress = new Progress(text + ' |:bar| :percent / :etas left', {
        total,
        width: null,
        complete: '█',
        incomplete: ' '
    })
    return () => progress.tick()
}

function csv() {
    let headerWritten = false
    return function* (record) {
        if (!headerWritten) {
            const header = PapaParse.unparse([Object.keys(record)])
            yield header + '\n'
            headerWritten = true
        }
        const entry = PapaParse.unparse([Object.values(record)])
        yield entry + '\n'
    }
}

async function write(filename) {
    await FSExtra.remove(filename)
    return contents => FSExtra.appendFile(filename, contents)
}

async function run() {
    const location = 'https://www.contractsfinder.service.gov.uk/Published/Notices/OCDS/Search?stages=award'
    const initial = Ix.AsyncIterable.from([location]).map(request)
    const length = await initial.map(response => Number(response.data.maxPage)).first()
    initial
        .flatMap(paginate)
        .map(request)
        .tap(ticker('Working...', length))
        .flatMap(detail)
        .flatMap(csv())
        .forEach(await write('contracts.csv'))
        .finally(() => console.log('Done!'))
}

run()
