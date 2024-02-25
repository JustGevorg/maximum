from enum import StrEnum

import psycopg2

RAW_SQL_SOLVE_QUERY = """
select distinct on (communication_id)
    communication_id, site_id, visitor_id, communication_date_time, visitor_session_id, session_date_time, campaign_id,
    max(row_n) over (partition by communication_id, site_id) as row_n
from (select c.communication_id,
             c.site_id,
             c.visitor_id,
             c.date_time                                                              as communication_date_time,
             s.visitor_session_id,
             s.date_time                                                              as session_date_time,
             s.campaign_id,
             row_number() over (partition by c.communication_id order by s.date_time) as row_n
      from web_data.communications as c
               left join web_data.sessions as s on c.visitor_id = s.visitor_id
      where c.site_id = s.site_id
      order by c.communication_id, s.date_time desc
      ) as tmp
where session_date_time < communication_date_time
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

    @classmethod
    def columns(cls) -> list[str]:
        return [v for v in cls.__dict__.get("_member_names_")]


class Sessions(ColumnsRetrieveStrEnum):
    visitor_session_id = "visitor_session_id"
    session_site_id = "session_site_id"
    session_visitor_id = "session_visitor_id"
    session_date_time = "session_date_time"
    campaign_id = "campaign_id"


class Communications(ColumnsRetrieveStrEnum):
    communication_id = "communication_id"
    communication_site_id = "communication_site_id"
    communication_visitor_id = "communication_visitor_id"
    communication_date_time = "communication_date_time"
