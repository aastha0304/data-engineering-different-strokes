source_db = dict(
    end_point="localhost",
    database="db",
    user="user",
    password="password"
)
sink_mysql = dict(
    end_point="dream11test.cluster-cefqnpdgdxcd.us-east-1.rds.amazonaws.com",
    database="dbd11live29june15",
    user="Dream11test",
    password="7Nng52(``N"
)
sink_redshift = dict(
    end_point="d11-reports-staging.cd2mebwblp0z.us-east-1.redshift.amazonaws.com",
    port="5439",
    database="dbd11",
    user="d11",
    password="jyFPzqqFKapMy2BM"
)
connect_url = "http://172.16.3.187:28083/connectors"
prefix = "reports_"
src_connector_name = "_prdsrc"
sink_mysql_connector_name = "cp_msink0"
sink_psql_connector_name = "_psink0"
