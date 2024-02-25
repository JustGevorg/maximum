import pandas as pd
import helpers
import config

from scripts import raw_sql_solve, pandas_solve


def main():
    conn = helpers.Connection.connect(database=config.DB_NAME,
                                      host=config.DB_HOST,
                                      user=config.DB_USER,
                                      password=config.DB_PASSWORD,
                                      port=config.DB_PORT)
    cursor = conn.cursor()

    pd.set_option('display.max_columns', None, "display.width", 200, "display.max_rows", None)

    raw_solve_result = raw_sql_solve(cursor, helpers.RAW_SQL_SOLVE_QUERY)
    pandas_solve_result = pandas_solve(cursor, helpers.GET_ALL_COMMUNICATIONS_QUERY, helpers.GET_ALL_SESSIONS_QUERY)

    diff = raw_solve_result.compare(pandas_solve_result)
    assert diff.empty, f"DataFrames aren't equal here: {diff}"


if __name__ == "__main__":
    main()
