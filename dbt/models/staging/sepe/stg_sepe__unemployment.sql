{{
  config(
    materialized='view',
    description='Staging layer for SEPE unemployment data - standardized and cleaned with municipality code completion'
  )
}}

-- Create a municipality name to code mapping from records that have both
with municipality_code_mapping as (
    select distinct
        upper(trim(municipality_name)) as municipality_name_clean,
        municipality_code
    from {{ source('raw', 'raw_sepe_unemployment') }}
    where municipality_code is not null 
      and municipality_name is not null
      and municipality_name != ''
),

-- Validate the mapping - ensure one-to-one relationship between names and codes
municipality_mapping_validated as (
    select 
        municipality_name_clean,
        municipality_code,
        count(*) over (partition by municipality_name_clean) as codes_per_name,
        count(*) over (partition by municipality_code) as names_per_code
    from municipality_code_mapping
),

-- Final mapping table (only include clean one-to-one mappings)
municipality_lookup as (
    select distinct
        municipality_name_clean,
        municipality_code
    from municipality_mapping_validated
    where codes_per_name = 1 and names_per_code = 1
),

source_data as (
    select 
        -- Geographic identifiers with municipality code completion
        coalesce(
            u.municipality_code,
            ml.municipality_code
        ) as municipality_code,
        u.municipality_name,
        
        -- Track whether municipality code was completed
        case 
            when u.municipality_code is null and ml.municipality_code is not null then true
            else false
        end as municipality_code_completed,
        
        -- Get province_code by joining with provinces mapping
        p.province_code,
        upper(trim(u.province)) as province_name,
        
        -- Date dimensions
        year as data_year,
        month as data_month,
        make_date(year::integer, month::integer, 1) as reporting_date,
        
        -- Total unemployment metrics (validated)
        case 
            when total_unemployment >= 0 then total_unemployment::integer
            else null 
        end as total_unemployment,
        
        -- Gender and age breakdown metrics (validated)
        case when men_under_25 >= 0 then men_under_25::integer else null end as men_under_25,
        case when men_25_44 >= 0 then men_25_44::integer else null end as men_25_44, 
        case when men_45_plus >= 0 then men_45_plus::integer else null end as men_45_plus,
        case when women_under_25 >= 0 then women_under_25::integer else null end as women_under_25,
        case when women_25_44 >= 0 then women_25_44::integer else null end as women_25_44,
        case when women_45_plus >= 0 then women_45_plus::integer else null end as women_45_plus,
        
        -- Calculated gender totals
        (coalesce(men_under_25, 0) + coalesce(men_25_44, 0) + coalesce(men_45_plus, 0))::integer as total_men_unemployment,
        (coalesce(women_under_25, 0) + coalesce(women_25_44, 0) + coalesce(women_45_plus, 0))::integer as total_women_unemployment,
        
        -- Sector breakdown metrics (validated)
        case when agriculture_sector >= 0 then agriculture_sector::integer else null end as agriculture_sector,
        case when industry_sector >= 0 then industry_sector::integer else null end as industry_sector,
        case when construction_sector >= 0 then construction_sector::integer else null end as construction_sector,
        case when services_sector >= 0 then services_sector::integer else null end as services_sector,
        case when no_previous_employment >= 0 then no_previous_employment::integer else null end as no_previous_employment,
        
        -- Data lineage
        source_file,
        data_source,
        data_source_full,
        data_category,
        ingestion_timestamp
        
    from {{ source('raw', 'raw_sepe_unemployment') }} u
    left join municipality_lookup ml
        on upper(trim(u.municipality_name)) = ml.municipality_name_clean
    left join {{ ref('provinces_autonomous_communities') }} p
        on upper(trim(u.province)) = upper(trim(p.province_name))
    
    -- Data quality filters
    where u.municipality_name is not null
      and u.municipality_name != ''
      and u.year between 2005 and 2025
      and u.month between 1 and 12
      and u.total_unemployment is not null
      and u.total_unemployment >= 0
),

final as (
    select 
        *,
        -- Additional calculated fields for analysis
        case 
            when total_unemployment > 0 then round((total_men_unemployment::numeric / total_unemployment::numeric) * 100, 2)
            else null 
        end as men_unemployment_percentage,
        
        case 
            when total_unemployment > 0 then round((total_women_unemployment::numeric / total_unemployment::numeric) * 100, 2)
            else null 
        end as women_unemployment_percentage,
        
        -- Age group percentages
        case 
            when total_unemployment > 0 then round(((coalesce(men_under_25, 0) + coalesce(women_under_25, 0))::numeric / total_unemployment::numeric) * 100, 2)
            else null 
        end as under_25_unemployment_percentage,
        
        case 
            when total_unemployment > 0 then round(((coalesce(men_25_44, 0) + coalesce(women_25_44, 0))::numeric / total_unemployment::numeric) * 100, 2)
            else null 
        end as age_25_44_unemployment_percentage,
        
        case 
            when total_unemployment > 0 then round(((coalesce(men_45_plus, 0) + coalesce(women_45_plus, 0))::numeric / total_unemployment::numeric) * 100, 2)
            else null 
        end as over_45_unemployment_percentage
        
    from source_data
)

select * from final