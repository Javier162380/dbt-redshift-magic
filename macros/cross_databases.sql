{% macro regenerate_database_schemas_cross_dbs(target_schemas, target_database) %}
    {% do log("Regenerating schemas " + target_schemas, info=True) %}
    {%- set db_schemas = target_schemas -%}
    {%- set db_name = target_database -%}
    {%- set database_objects_query -%}
        select distinct 
            database_name || '.' ||schema_name || '.' || table_name as object_name
        from svv_redshift_tables
        where table_type = 'TABLE'
            and database_name = {{db_name}}
            and schema_name in ({{db_schemas}})
    {% endset %}
    {% do log(database_objects_query, info=True) %}
    {%- set database_objects = run_query(database_objects_query) -%}
    {%- for database_object in  database_objects.columns[0].values() %}
        {%- set database_name, table_schema, table_name = database_object.split('.') -%}
        {%- set table_object = table_schema + '.' + table_name -%}
        {% do log("Regenerating sandbox object " + table_object, info=True) %} 
        {%- set regeneration_query -%}
                CREATE SCHEMA IF NOT EXISTS {{target_schema}}; 
                DROP TABLE IF EXISTS {{table_object}} CASCADE;
                CREATE TABLE {{table_object}} as (
                    SELECT 
                        * 
                    FROM {{database_object}}
                )
        {% endset %}
        {% do log(regeneration_query, info=True) %}
        {% do run_query(regeneration_query) %}
        {% do log("Sandbox object " + table_object + " regenerated succesfully!", info=True) %}
    {%- endfor %}
    {% do log("Sandbox objects regenerated succesfully!", info=True) %}
{% endmacro %}

{% macro regenerate_database_schemas_sample_cross_dbs(target_schemas, target_database, sample_size) %}
    {% do log("Regenerating schemas " + target_schemas, info=True) %}
    {%- set db_schemas = target_schemas -%}
    {%- set db_name = target_database -%}
    {%- set db_sample = sample_size -%}
    {%- set database_objects_query -%}
        select distinct 
            database_name || '.' ||schema_name || '.' || table_name as object_name
        from svv_redshift_tables
        where table_type = 'TABLE'
            and database_name = {{db_name}}
            and schema_name in ({{db_schemas}})
    {% endset %}
    {% do log(database_objects_query, info=True) %}
    {%- set database_objects = run_query(database_objects_query) -%}
    {%- for database_object in  database_objects.columns[0].values() %}
        {%- set database_name, table_schema, table_name = database_object.split('.') -%}
        {%- set table_object = table_schema + '.' + table_name -%}
        {% do log("Regenerating sandbox object " + table_object, info=True) %} 
        {%- set regeneration_query -%}
                CREATE SCHEMA IF NOT EXISTS {{target_schema}};  
                DROP TABLE IF EXISTS {{table_object}} CASCADE;
                CREATE TABLE {{table_object}} as (
                    SELECT 
                        * 
                    FROM {{database_object}}
                    LIMIT {{db_sample}}
                )
        {% endset %}
        {% do log(regeneration_query, info=True) %}
        {% do run_query(regeneration_query) %}
        {% do log("Sandbox object " + table_object + " regenerated succesfully!", info=True) %}
    {%- endfor %}
    {% do log("Sandbox objects regenerated succesfully!", info=True) %}
{% endmacro %}

{% macro regenerate_database_table_cross_dbs(target_schema, target_table, target_database) %}
    {% do log("Regenerating object " + target_table, info=True) %}
    {%- set db_schema = target_schema -%}
    {%- set db_name = target_database -%}
    {%- set db_table = target_table -%}
    {%- set database_objects_query -%}
            select distinct 
                table_name as object_name
            from svv_redshift_tables
            where table_type = 'TABLE'
                and table_name = '{{db_table}}'
                and database_name = '{{db_name}}'
                and schema_name = '{{db_schema}}'
    {% endset %}
    {% do log(database_objects_query, info=True) %}
    {%- set database_object = run_query(database_objects_query) -%}
    {%- set table_exists = database_object.columns[0].values()[0] == target_table -%}
    {% if table_exists == True %}
        {% do log("Database object exists!") %}
        {%- set regeneration_query -%}
        CREATE SCHEMA IF NOT EXISTS {{target_schema}};
        DROP TABLE IF EXISTS {{target_schema}}.{{target_table}} CASCADE;
        CREATE TABLE {{target_schema}}.{{target_table}} AS (
            SELECT 
                *
            FROM {{target_database}}.{{target_schema}}.{{target_table}}
        )
        {% endset %}
        {% do log(regeneration_query, info=True) %}
        {% do run_query(regeneration_query) %}
        {% do log("Database object " + target_schema + "." + target_table + " was created succesfully!", info=True) %}
    {% else %}
        {% do log("Database object " + target_schema + "." + target_table + " does not exists", info=True) %}
    {% endif %}
{% endmacro %}

{% macro regenerate_database_table_sample_cross_dbs(target_schema, target_table, target_database) %}
    {% do log("Regenerating object " + target_table, info=True) %}
    {%- set db_schema = target_schema -%}
    {%- set db_name = target_database -%}
    {%- set db_table = target_table -%}
    {%- set database_objects_query -%}
            select distinct 
                table_name as object_name
            from svv_redshift_tables
            where table_type = 'TABLE'
                and table_name = '{{db_table}}'
                and database_name = '{{db_name}}'
                and schema_name = '{{db_schema}}'
    {% endset %}
    {% do log(database_objects_query, info=True) %}
    {%- set database_object = run_query(database_objects_query) -%}
    {%- set table_exists = database_object.columns[0].values()[0] == target_table -%}
    {% if table_exists == True %}
        {% do log("Database object exists!") %}
        {%- set regeneration_query -%}
        CREATE SCHEMA IF NOT EXISTS {{target_schema}};
        DROP TABLE IF EXISTS {{target_schema}}.{{target_table}} CASCADE;
        CREATE TABLE {{target_schema}}.{{target_table}} AS (
            SELECT 
                *
            FROM {{target_database}}.{{target_schema}}.{{target_table}}
        )
        {% endset %}
        {% do log(regeneration_query, info=True) %}
        {% do run_query(regeneration_query) %}
        {% do log("Database object " + target_schema + "." + target_table + " was created succesfully!", info=True) %}
    {% else %}
        {% do log("Database object " + target_schema + "." + target_table + " does not exists nothing was regenerated", info=True) %}
    {% endif %}
{% endmacro %}

{% macro regenerate_database_table_sample_cross_dbs(target_schema, target_table, target_database, sample_size) %}
    {% do log("Regenerating object " + target_table, info=True) %}
    {%- set db_schema = target_schema -%}
    {%- set db_name = target_database -%}
    {%- set db_table = target_table -%}
    {%- set db_sample = sample_size -%}
    {%- set database_objects_query -%}
            select distinct 
                table_name as object_name
            from svv_redshift_tables
            where table_type = 'TABLE'
                and table_name = '{{db_table}}'
                and database_name = '{{db_name}}'
                and schema_name = '{{db_schema}}'
                LIMIT {{db_sample}}
    {% endset %}
    {% do log(database_objects_query, info=True) %}
    {%- set database_object = run_query(database_objects_query) -%}
    {%- set table_exists = database_object.columns[0].values()[0] == target_table -%}
    {% if table_exists == True %}
        {% do log("Database object exists!") %}
        {%- set regeneration_query -%}
        CREATE SCHEMA IF NOT EXISTS {{target_schema}};
        DROP TABLE IF EXISTS {{target_schema}}.{{target_table}} CASCADE;
        CREATE TABLE {{target_schema}}.{{target_table}} AS (
            SELECT 
                *
            FROM {{target_database}}.{{target_schema}}.{{target_table}}
        )
        {% endset %}
        {% do log(regeneration_query, info=True) %}
        {% do run_query(regeneration_query) %}
        {% do log("Database object " + target_schema + "." + target_table + " was created succesfully!", info=True) %}
    {% else %}
        {% do log("Database object " + target_schema + "." + target_table + " does not exists", info=True) %}
    {% endif %}
{% endmacro %}