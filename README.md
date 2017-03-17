Scrape Contracts
================

The [Crown Commercial Service](https://www.gov.uk/government/organisations/crown-commercial-service), the body responsible for awarding public sector contracts, publishes that data on [Contracts Finder](https://www.contractsfinder.service.gov.uk/).

This uses [the Contracts Finder API](https://www.gov.uk/government/publications/open-contracting) to pull all the available records into a CSV file.

Requires [Node](https://nodejs.org/).

Install the dependencies with `npm install`, then run `node contracts`. Produces a file named `contracts.csv`.
