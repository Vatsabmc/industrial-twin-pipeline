-- macros/generate_schema_name.sql
-- Overrides dbt's default schema naming so that in production the
-- dataset name is used as-is (from dbt_project.yml), rather than
-- prefixed with the profile target schema.
-- This is the standard approach taught in the DE Zoomcamp dbt module.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
