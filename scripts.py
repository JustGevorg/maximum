from typing import Iterable

from helpers import Communications, Sessions, RESULT_COLUMNS
import pandas as pd


def _query_result_to_df(cursor, *queries: str) -> list[pd.DataFrame]:
    """
    transform query results to lis of pd.DataFrame
    :param cursor: object to execute queries
    :param queries: sql query to run
    :return: list of queries results as pd.DataFrame objects
    """
    dfs = []
    for query in queries:
        cursor.execute(query)
        dfs.append(pd.DataFrame(cursor.fetchall()))

    return dfs


def raw_sql_solve(cursor, query: str) -> pd.DataFrame:
    """solve task with just execute sql query and return dataframe with result"""
    df, = _query_result_to_df(cursor, query)
    df.columns = RESULT_COLUMNS

    return df


def pandas_solve(cursor, query_all_communications: str, query_all_sessions: str) -> pd.DataFrame:
    """
    solve same task through pandas and return dataframe with result
    :param cursor: object to execute queries
    :param query_all_communications: retrieve all data from web_data.communications
    :param query_all_sessions: retrieve all data from web_data.sessions
    :return: dataframe with target data
    """

    sessions_df, communications_df = _query_result_to_df(cursor, query_all_sessions, query_all_communications)
    sessions_df.columns = list(Sessions)
    communications_df.columns = list(Communications)

    result = pd.merge(communications_df, sessions_df,
                      left_on=Communications.communication_visitor_id,
                      right_on=Sessions.session_visitor_id, how='left', suffixes=(None, None))

    result.sort_values(by=[Communications.communication_id, Sessions.session_date_time], ascending=False)
    result["row_n"] = result.sort_values([Communications.communication_id, Sessions.session_date_time],
                                         ascending=True).groupby(Communications.communication_id).cumcount() + 1
    result.loc[result[Communications.communication_site_id] != result[Sessions.session_site_id], Sessions.visitor_session_id] = None
    result.loc[result[Communications.communication_site_id] != result[Sessions.session_site_id], Sessions.session_date_time] = None
    result.loc[result[Communications.communication_site_id] != result[Sessions.session_site_id], Sessions.campaign_id] = None
    result.loc[result[Communications.communication_site_id] != result[Sessions.session_site_id], "row_n"] = None

    result = result.loc[(result[Communications.communication_date_time] > result[Sessions.session_date_time]) | (result[Sessions.session_date_time] == None)]

    result = result.groupby(Communications.communication_id, group_keys=False).apply(lambda x: x.loc[x.row_n.idxmax()])
    result.drop(columns=[Sessions.session_site_id, Sessions.session_visitor_id], inplace=True)
    result.reset_index(drop=True, inplace=True)

    result.rename(columns={Communications.communication_site_id: 'site_id',
                           Communications.communication_visitor_id: "visitor_id"}, inplace=True)

    cols = [Sessions.visitor_session_id, Sessions.campaign_id, "row_n"]
    result[cols] = result[cols].applymap(int)
    return result
