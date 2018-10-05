import sys
import psycopg2


def execute_query(query):
    # Establish connection with database and execute the query passed to it.

    try:
        conn = psycopg2.connect('dbname=news')
        c = conn.cursor()
        c.execute(query)  # The SQL query to execute.
        rows = c.fetchall()
        conn.close()
        return rows  # A list of resultant rows.
    except psycopg2.Error as e:
        print(e)
        sys.exit(1)


def get_popular_articles():
    """Return top three popular articles of all time."""
    query = """
        SELECT articles.title, COUNT(*) AS num
        FROM articles
        JOIN log
        ON log.path LIKE concat('/article/%', articles.slug)
        GROUP BY articles.title
        ORDER BY num DESC
        LIMIT 3;
    """

    # Execute the above query.
    results = execute_query(query)

    # Print results.
    print("1. The 3 most popular articles of all time are:")
    rank = 1
    for row in results:
        print(u"  {0}. \"{1}\" — {2:,} views.".format(rank, row[0], row[1]))
        rank += 1


def get_popular_authors():
    """Return authors sorted by page views."""
    query = """
        SELECT authors.name,COUNT(*) as num
        FROM authors
        JOIN
        articles
        ON articles.author=authors.id
        JOIN
        log on log.path LIKE concat('/article/%',articles.slug)
        GROUP BY authors.name
        ORDER BY num DESC;
    """

    # Run above query.
    results = execute_query(query)

    # Print results.
    print("2. The most popular article authors of all time are:")
    rank = 1
    for row in results:
        print(u"  {0}. {1} — {2:,} views.".format(rank, row[0], row[1]))
        rank += 1


def get_days_with_errors():
    """Return days in which more than 1% requests lead to errors."""
    query = """
        SELECT total.time,
        ROUND(((errors.err_req*100.0) / total.req), 3) AS percent
        FROM (
        SELECT time::timestamp::date, count(*) AS err_req
        FROM log
        WHERE status LIKE '404%'
        GROUP BY time::timestamp::date
        ) AS errors
        JOIN (
        SELECT time::timestamp::date, count(*) AS req
        FROM log
        GROUP BY time::timestamp::date
        ) AS total
        ON total.time::timestamp::date = errors.time::timestamp::date
        WHERE (ROUND(((errors.err_req*100.0) / total.req), 3) > 1)
        ORDER BY percent DESC;err_requests * 100.0)/total.requests), 5) > 1.0)
        ORDER BY percent DESC;
    """

    # Execute the above query.
    results = execute_query(query)

    # Print results.
    print("3. Days with more than 1% of request that lead to an error:")
    for row in results:
        date = row[0].strftime('%B %d, %Y')     # Pretty-formatting date.
        errors = str(round(row[1], 2)) + "%" + " errors"
        print("   " + date + u" — " + errors)


# Print all the results.
if __name__ == '__main__':
    get_popular_articles()
    get_popular_authors()
    get_days_with_errors()
