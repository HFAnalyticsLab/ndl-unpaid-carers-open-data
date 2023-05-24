# Analysis of Understanding Society and open data on unpaid carers

#### Project Status: In progress

## Project Description

For our first Networked Data Lab output on unpaid carers, we analysed survey data from Understanding Society as well as additional open sources of data from ONS and NHS Digital.

## Data sources

* The raw Understanding Society survey data can be download from the UK Data Service. The version we use is hosted [here](https://beta.ukdataservice.ac.uk/datacatalogue/studies/study?id=6614). Please refer to the code in [this repository](https://github.com/HFAnalyticsLab/understanding-society) to see how the raw survey data is pre-processsed before this analysis.
* [Adult Social Care Activity and Finance Report, NHS Digital](https://digital.nhs.uk/data-and-information/publications/statistical/adult-social-care-activity-and-finance-report)
* [England and Wales Census 2011, ONS](https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/socialcare/datasets/unpaidcarebyageandsexenglandandwales)
* [England and Wales Census 2021, unstandardised, ONS](https://www.ons.gov.uk/datasets/TS039/editions/2021/versions/2)
* [England and Wales Census 2021,  age-standardised proportions, ONS](https://www.ons.gov.uk/datasets/TS039ASP/editions/2021/versions/2)
* [Number of carers' allowance claimants, Department for Work and Pensions via Stat-Xplore](https://stat-xplore.dwp.gov.uk/webapi/jsf/login.xhtml)

## How does it work?

This repository includes two separate folders:

**Understanding Society analysis**

* **`1. Create variables carers.R`** This file starts with a clean Understanding Society file in long format called 'USOC long.csv'. Then, it proceeds to create the new variables required for our analysis on unpaid carers. Some of these new variables (e.g. 'relationship to cared-for person' or 'maximum age of cared-for people') require a 'linkage' between carers and the people they care for in their households. Because this is a large survey and dataset, it can take ~30 minutes to run even after parallelizing these computations. If you wish to add variables about people's health conditions, please add this portion of data (**`Coding medical conditions with Usoc.R`**) back into the file.

* **`2. Summary statistics.R`** Using this file, we take our Understanding Society data with our new carer variables and we apply survey weights before producing various descriptive statistics.

**Other open data analysis**

* **`Other open data analysis on carers/Open data analysis on carers for long chart.R`** Using this file, we produce additional statistics using data from the ONS, NHS Digital and DWP.

* **`NDL lab analysis/NDL areas census analysis/NDL areas census analysis.Rmd`** Analysis of hours spent caring and age of carers using 2021 Census data.

**Analysis of NDL partner data**

* **`NDL lab analysis/Local analyses.Rmd`** Summary of central analyses on unpaid carers produced by NDL partners.

### Requirements

These scripts were written in R version 4.0.2.

In order to create the new carers variables, you will need the following packages to enable parallel computing:

* parallel
* pbmcapply

Otherwise, please adapt the code to run the commands on a single core.

You will need to install the following R packages for the survey data analysis:

* survey
* gtsummary
* srvyr

For the rest of the open source data analysis, this package will allow you to query the ONS API to get population denominators:

* onsr

## Authors

* Sebastien Peytrignet - [Twitter](https://twitter.com/SebastienPeytr2) - [GitHub](https://github.com/sg-peytrignet)

## License

This project is licensed under the MIT License.

## Acknowledgments

Thank you to Fiona Grimm ([GitHub](https://github.com/fiona-grimm)), my co-author, for her advice and guidance throughout this analysis.
