from enum import StrEnum, auto

import psycopg2

RAW_SQL_SOLVE_QUERY = """
select distinct on (communication_id)
    communication_id, site_id, visitor_id, communication_date_time, visitor_session_id, session_date_time, campaign_id,
    max(row_n) over (partition by communication_id, site_id) as row_n
from (select c.communication_id,
             c.site_id,
             c.visitor_id,
             c.date_time                                                              as communication_date_time,
             case when c.site_id <> s.site_id then null else s.visitor_session_id end as visitor_session_id,
             case when c.site_id <> s.site_id then null else s.date_time end as session_date_time,
             case when c.site_id <> s.site_id then null else s.campaign_id end as campaign_id,
             case when c.site_id <> s.site_id then null else row_number() over (partition by c.communication_id order by s.date_time) end as row_n
      from web_data.communications as c
               left join web_data.sessions as s on c.visitor_id = s.visitor_id
      order by c.communication_id, s.date_time desc
      ) as tmp
where session_date_time < communication_date_time or session_date_time is NULL
"""

GET_ALL_SESSIONS_QUERY = """
SELECT * FROM web_data.sessions;
"""

GET_ALL_COMMUNICATIONS_QUERY = """
SELECT * FROM web_data.communications;
"""

RESULT_COLUMNS = ["communication_id", "site_id", "visitor_id", "communication_date_time",
                  "visitor_session_id", 'session_date_time', "campaign_id", "row_n"]


class Connection:
    def connect(*args, **kwargs):
        with psycopg2.connect(**kwargs) as conn:
            return conn


class ColumnsRetrieveStrEnum(StrEnum):
    """field_name-only enum"""
    def _generate_next_value_(name, start, count, last_values):
        return name


class Sessions(ColumnsRetrieveStrEnum):
    visitor_session_id = auto()
    session_site_id = auto()
    session_visitor_id = auto()
    session_date_time = auto()
    campaign_id = auto()


class Communications(ColumnsRetrieveStrEnum):
    communication_id = auto()
    communication_site_id = auto()
    communication_visitor_id = auto()
    communication_date_time = auto()
