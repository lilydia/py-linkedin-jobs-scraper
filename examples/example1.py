from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters


def on_data(data: EventData):
    print('[ON_DATA]', data.job_function)


def on_error(error):
    print('[ON_ERROR]', error)


def on_end():
    print('[ON_END]')


scraper = LinkedinScraper(
    chrome_options=None,  # You can pass your custom Chrome options here
    max_workers=1,  # How many threads will be spawn to run queries concurrently (one Chrome driver for each thread)
    slow_mo=0.4,  # Slow down the scraper to avoid 'Too many requests (429)' errors
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
        query='Engineer',
        options=QueryOptions(
            locations=['Toronto, Ontario, Canada'],
            optimize=False,
            limit=5,
            filters=QueryFilters(
                relevance=RelevanceFilters.RECENT,
                time=TimeFilters.MONTH,
                type=[TypeFilters.FULL_TIME, TypeFilters.INTERNSHIP],
                experience=None,
            )
        )
    ),
]

scraper.run(queries)
