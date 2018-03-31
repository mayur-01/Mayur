import psycopg2 as psql
from jinja2 import Template
import timeit

clients=['labellife']

level1={ 
        'one_d1':['os','browser','userevent','pagetype','vendor'],
        'one_d2':['session_id','uuid','sourceprodid'],
        'levels':['date','month','overall'],
        'action':{
                'date':'date',
                'month':" cast(extract(year from date)||'-'||extract(month from date)||'-01' as date)",
                'overall':"cast('1991-09-26' as date)" }
                     
        }

level2={ 
        'two_d1':[('browser','os'),('vendor','userevent'),('userevent','pagetype')],
        'two_d2':[('os','uuid'), ('os','session_id'),
                  ('browser','uuid'), ('browser','session_id'), 
                  ('vendor','uuid'), ('vendor','session_id'),
                  ('userevent','uuid'), ('userevent','session_id'), ('userevent','sourceprodid')],
        'levels':['date','month','overall'],
        'action':{
                   'date':'date',
                   'month':" cast(extract(year from date)||'-'||extract(month from date)||'-01' as date)",
                   'overall':" cast('1991-09-26' as date)" } 
        }


def make_connection():
    conn_string="dbname='msd' port='5439' user='db_runner' password='MSD_dashboard_42' host='optimizedcluster.czunimvu3pdw.us-west-2.redshift.amazonaws.com'"
    connection = psql.connect(conn_string)
    redshift_cursor = connection.cursor()
    return connection, redshift_cursor


eda_level1=""" 
create temp table {{clients}}_eda_level1 as 
select  time_level,
        time_value,
        variable_level,
        variable_name,
        cast(variable_value as varchar(200)),
        counts
from (
{% for col in one_d1 %}  {% for level in levels %} 
select         '{{level}}' as time_level,
                {{action[level]}} as time_value,
               '1' as variable_level,
               '{{col}}' as variable_name,
               "{{col}}" as variable_value,
                count(*) as counts 
from {{clients}}.events
group by 1,2,3,4,5 
 union {% endfor %} {% endfor %}
  
{% for co in one_d2 %} {% for level in levels %}
select        ' {{level}}' as time_level,
                {{action[level]}} as time_value,
                '1' as variable_level,
               '{{co}}' as variable_name,
               '{{co}}' as variable_value,
              count(distinct {{co}}) as counts      
from {{clients}}.events
group by 1,2,3,4,5 
{% if co==one_d2[-1] and level==levels[-1] %} ) {% else %} union {% endif %}{% endfor %} {% endfor %}
 """      

eda_level2 ="""   
create temp table {{clients}}_eda_level2 as
select    time_level,
          time_value,
          variable_level,
          variable_name,
          cast(variable_value as varchar(200)),
          counts
from (

{% for col,header in two_d1 %} {% for level in levels %}
select    '{{level}}' as time_level, 
           {{action[level]}} as time_value,
            '2' as variable_level,
            '{{col}} & {{header}}' as variable_name,
            "{{col}}"+ '<->' + "{{header}}" as variable_value, 
              count(*) as counts 
from {{clients}}.events
group by 1,2,3,4,5
 union  {% endfor %}  {% endfor %}

{% for co,head in two_d2 %} {% for level in levels %}

 select         '{{level}}' as time_level, 
                 {{action[level]}} as time_value,
                 '2' as variable_level,
                 '{{co}} & {{head}}' as variable_name,
                 "{{co}}"+ ' <-> ' + '{{head}}' as variable_value,
                  count(distinct {{head}}) as counts 
                    
from {{clients}}.events
group by 1,2,3,4,5 
{% if head==two_d2[-1][-1] and co==two_d2[-1][0] and level==levels[-1] %} ) {% else %} union {% endif %}{% endfor %} {% endfor %}
"""      

drop_eda= """
drop table if exists {{clients}}_summary
"""

eda= """ 
create table {{clients}}_summary as
select *
from  (
        select * from {{clients}}_eda_level1
        union
        select * from {{clients}}_eda_level2
        ) as a          
"""


def create_eda_table(client):
    con,cur = make_connection()

    then = timeit.default_timer()
    eda_level1_query = Template(eda_level1).render(clients=client,**level1)
    print eda_level1_query
    cur.execute(eda_level1_query)
    print timeit.default_timer()-then, "level 1 computation time in seconds"

    then = timeit.default_timer()
    eda_level2_query = Template(eda_level2).render(clients=client,**level2)
    print eda_level2_query
    cur.execute(eda_level2_query)
    print timeit.default_timer()-then, "level 2 computation time in seconds"

    then = timeit.default_timer()
    drop_eda_query = Template(drop_eda).render(clients=client)
    print drop_eda_query
    cur.execute(drop_eda_query)
    print timeit.default_timer()-then, "EDA Drop computation time in seconds"

    then = timeit.default_timer()
    eda_query = Template(eda).render(clients=client)
    print eda_query
    cur.execute(eda_query)
    print timeit.default_timer()-then, "EDA computation time in seconds"

    cur.execute('commit')

if __name__ == '__main__':
    for client in clients:
        print client
        create_eda_table(client)
        print client, ": Done\n\n\n\n"