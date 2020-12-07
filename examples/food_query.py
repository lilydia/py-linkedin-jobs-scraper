from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters
import csv
import pandas

searches = ['chef', 'bartender', 'server and food']

def on_data(data: EventData):
    #print('[ON_DATA]', data.title, data.company, data.date, data.link, len(data.description))
    title.append(data.title)
    company.append(data.company)
    date.append(data.date)
    link.append(data.link)

def on_error(error):
    print('[ON_ERROR]', error)


def on_end():
    print('[ON_END]')


for counter in range(2):
    if counter == 0: 
        for search in searches:

            title =[]
            company = []
            date = []
            link = []
            industry = []
            occupation = []
            jobType = []

            scraper = LinkedinScraper(
                chrome_options=None,  # You can pass your custom Chrome options here
                max_workers=1,  # How many threads will be spawn to run queries concurrently (one Chrome driver for each thread)
                slow_mo=0.6,  # Slow down the scraper to avoid 'Too many requests (429)' errors
            )

            # Add event listeners
            scraper.on(Events.DATA, on_data)
            scraper.on(Events.ERROR, on_error)
            scraper.on(Events.END, on_end)

            queries = [
                Query(
                    options=QueryOptions(
                        optimize=True,  # Blocks requests for resources like images and stylesheet
                        limit=0  # Limit the number of jobs to scrape
                    )
                ),
                Query(
                    query= search ,
                    options=QueryOptions(
                        locations=['Toronto, Ontario, Canada'],
                        optimize=True,
                        limit=200,
                        filters=QueryFilters(
                            relevance=RelevanceFilters.RELEVANT,
                            time=TimeFilters.WEEK,
                            type=[TypeFilters.FULL_TIME],
                        )
                    )
                ),
            ]

            scraper.run(queries)

            for i in range(len(title)):
                industry.append('Food')
                occupation.append(search)
                jobType.append('Full Time')

            df = pandas.DataFrame(data={"Title": title, "Company": company, "Date": date, "Link":link, "Industry":industry, "Occupation":occupation, "Type": jobType})
            df.to_csv("C:/Users/User/py-linkedin-jobs-scraper/examples/food.csv", mode='a', sep=',',header=False, index=False)

    if counter == 1: 
        for search in searches:

            title =[]
            company = []
            date = []
            link = []
            industry = []
            occupation = []
            jobType = []

            scraper = LinkedinScraper(
                chrome_options=None,  # You can pass your custom Chrome options here
                max_workers=1,  # How many threads will be spawn to run queries concurrently (one Chrome driver for each thread)
                slow_mo=0.6,  # Slow down the scraper to avoid 'Too many requests (429)' errors
            )

            # Add event listeners
            scraper.on(Events.DATA, on_data)
            scraper.on(Events.ERROR, on_error)
            scraper.on(Events.END, on_end)

            queries = [
                Query(
                    options=QueryOptions(
                        optimize=True,  # Blocks requests for resources like images and stylesheet
                        limit=0  # Limit the number of jobs to scrape
                    )
                ),
                Query(
                    query= search ,
                    options=QueryOptions(
                        locations=['Toronto, Ontario, Canada'],
                        optimize=True,
                        limit=50,
                        filters=QueryFilters(
                            relevance=RelevanceFilters.RELEVANT,
                            time=TimeFilters.WEEK,
                            type=[TypeFilters.PART_TIME],
                        )
                    )
                ),
            ]

            scraper.run(queries)

            for i in range(len(title)):
                industry.append('Food')
                occupation.append(search)
                jobType.append('Part Time')

            df = pandas.DataFrame(data={"Title": title, "Company": company, "Date": date, "Link":link, "Industry":industry, "Occupation":occupation, "Type": jobType})
            df.to_csv("C:/Users/User/py-linkedin-jobs-scraper/examples/food.csv", mode='a', sep=',',header=False, index=False)
