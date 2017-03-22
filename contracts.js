const Highland = require('highland')
const Request = require('request')
const RetryMe = require('retry-me')
const FS = require('fs')
const CSVWriter = require('csv-write-stream')

const http = Highland.wrapCallback((location, callback) => {
    const input = output => {
        Request(location, (error, response) => {
            const failure = error ? error : (response.statusCode >= 400) ? new Error(response.statusCode) : null
            output(failure, response)
        })
    }
    RetryMe(input, { factor: 1.5 }, callback)
})

const location = {
    uri: 'https://www.contractsfinder.service.gov.uk/Published/Notices/OCDS/Search',
    qs: {
        'stages': 'award'
    }
}

function pages(response) {
    const pages = JSON.parse(response.body).maxPage
    return Array.from({ length: pages }).map((_, i) => {
        const page = i + 1
        return {
            uri: 'https://www.contractsfinder.service.gov.uk/Published/Notices/OCDS/Search',
            qs: {
                'stages': 'award',
                'page': page
            },
            page
        }
    })
}

function results(response) {
    console.log('Processing page ' + response.request.page + '...')
    const data = JSON.parse(response.body)
    return data.results.map(result => {
        return result.releases.map(release => {
            if (release.awards === undefined) return []
            return release.awards.map(award => {
                return award.suppliers.map(supplier => {
                    return {
                        supplierName: supplier.name,
                        supplierAddress: [
                            supplier.address.streetAddress,
                            supplier.address.locality,
                            supplier.address.region,
                            supplier.address.postalCode,
                            supplier.address.countryName
                        ].filter(x => x).join(', '),
                        supplierContactName: supplier.contactPoint.name,
                        awardValue: award.value.amount,
                        awardGiven: award.date,
                        awardStart: award.contractPeriod ? award.contractPeriod.startDate : '',
                        awardEnd: award.contractPeriod ? award.contractPeriod.endDate : '',
                        buyerName: release.buyer.name,
                        buyerAddress: [
                            release.buyer.address.streetAddress,
                            release.buyer.address.locality,
                            release.buyer.address.region,
                            release.buyer.address.postalCode,
                            release.buyer.address.countryName
                        ].filter(x => x).join(', '),
                        buyerContactName: release.buyer.contactPoint.name,
                        buyerContactEmail: release.buyer.contactPoint.email,
                        buyerContactPhone: release.buyer.contactPoint.telephone,
                        buyerContactUrl: release.buyer.contactPoint.uri,
                        contractTitle: release.tender.title,
                        contractDescription: release.tender.description,
                        contractValue: release.tender.minValue.amount,
                        contractValueMin: release.tender.minValue.amount,
                        contractPublished: result.publishedDate
                    }
                })
            })
        })
    })
}

Highland([location])
    .flatMap(http)
    .flatMap(pages)
    .flatMap(http)
    .flatMap(results)
    .flatten()
    .errors(e => console.error(e.stack))
    .through(CSVWriter())
    .pipe(FS.createWriteStream('contracts.csv'))
