#!/usr/bin/env python3

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
        ON log.path = concat('/article/', articles.slug)
        GROUP BY articles.title
        ORDER BY num DESC
        LIMIT 3;
    """

    # Execute the above query.
    rows = execute_query(query)

    # Print results.
    print("-" * 70)
    print("1. The 3 most popular articles of all time are:")
    for (title, count) in rows:
        '''
        (title, count) unpacks each row into
        a tuple and you can refer to them easily
        in your loop block as shown in the next line.
        '''
        print("    {} - {} views".format(title, count))
    print("-" * 70)


def get_popular_authors():
    """Return authors sorted by page views."""
    query = """
        SELECT authors.name,COUNT(*) as num
        FROM authors
        JOIN
        articles
        ON articles.author=authors.id
        JOIN
        log on log.path = concat('/article/',articles.slug)
        GROUP BY authors.name
        ORDER BY num DESC;
    """

    # Run above query.
    rows = execute_query(query)

    # Print results.
    print("2. The most popular article authors of all time are:")
    for (title, count) in rows:
        print("    {} - {} views".format(title, count))
    print("-" * 70)


def get_days_with_errors():
    """Return days in which more than 1% requests lead to errors."""
    query = """
    select to_char(date, 'FMMonth FMDD, YYYY'), err/total as ratio
    from (select time::date as date,
    count(*) as total,
    sum((status != '200 OK')::int)::float as err
    from log
    group by date) as errors
    where err/total > 0.01

    """

    # Execute the above query.
    results = execute_query(query)

    # Print results.
    print("3. Days with more than 1% of request that lead to an error:")
    for row in results:
        date = row[0]  # Pretty-formatting date.
        errors = str("%.2f" % float(row[1] * 100)) + "%" + " errors"
        print("   " + date + u" — " + errors)
    print("-" * 70)


# Print all the results.
if __name__ == '__main__':
    get_popular_articles()
    get_popular_authors()
    get_days_with_errors()
