{% macro regenerate_database_schemas_cross_clusters(target_schemas, target_database) %}
    {% do log("Regenerating schemas " + target_schemas, info=True) %}
    {%- set db_schemas = target_schemas -%}
    {%- set db_name = target_database -%}
    {%- set datashare_objects -%}
        select distinct 
            share_name || '.' || object_name as table_name
        from svv_datashare_objects
        where object_type not in ('schema', 'late binding view')
              and 
              split_part(object_name, '.', 1) in ({{db_schemas}})
    {% endset %}

    {%- set datashare_objects = run_query(datashare_objects) -%}

    {%- for datashare_object in  datashare_objects.columns[0].values() %}

        {%- set datashare, table_schema, table_name = datashare_object.split('.') -%}
        {%- set table_object = table_schema + '.' + table_name -%}
        {% do log("Regenerating sandbox object " + table_object + " from datashare " + datashare, info=True) %} 
        {%- set regeneration_query -%} 
                CREATE SCHEMA IF NOT EXISTS {{table_schema}}; 
                DROP TABLE IF EXISTS {{table_object}} CASCADE;
                CREATE TABLE {{table_object}} as (
                    SELECT 
                        * 
                    FROM {{datashare_object}}
                )
        {% endset %}
        {% do log(regeneration_query, info=True) %}
        {% do run_query(regeneration_query) %}
        {% do log("Sandbox object " + table_object + " regenerated succesfully!", info=True) %}
    {%- endfor %}
    {% do log("Sandbox objects regenerated succesfully!", info=True) %}
{% endmacro %}

{% macro regenerate_database_schemas_sample_cross_clusters(target_schemas, target_database, sample_size) %}
    {% do log("Regenerating schemas " + target_schemas, info=True) %}
    {%- set db_schemas = target_schemas -%}
    {%- set db_name = target_database -%}
    {%- set db_sample = sample_size -%}
    {%- set datashare_objects -%}
        select distinct 
            share_name || '.' || object_name as table_name
        from svv_datashare_objects
        where object_type not in ('schema', 'late binding view')
              and 
            split_part(object_name, '.', 1) in ({{db_schemas}})
    {% endset %}

    {%- set datashare_objects = run_query(datashare_objects) -%}

    {%- for datashare_object in  datashare_objects.columns[0].values() %}

        {%- set datashare, table_schema, table_name = datashare_object.split('.') -%}
        {%- set table_object = table_schema + '.' + table_name -%}
        {% do log("Regenerating sandbox object " + table_object + " from datashare " + datashare, info=True) %} 
        {%- set regeneration_query -%} 
                CREATE SCHEMA IF NOT EXISTS {{table_schema}}; 
                DROP TABLE IF EXISTS {{table_object}} CASCADE;
                CREATE TABLE {{table_object}} as (
                    SELECT 
                        * 
                    FROM {{datashare_object}}
                    LIMIT {{db_sample}}
                )
        {% endset %}
        {% do log(regeneration_query, info=True) %}
        {% do run_query(regeneration_query) %}
        {% do log("Sandbox object " + table_object + " regenerated succesfully!", info=True) %}
    {%- endfor %}
    {% do log("Sandbox objects regenerated succesfully!", info=True) %}

{% endmacro %}