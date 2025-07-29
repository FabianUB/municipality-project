{{
  config(
    materialized='view',
    description='Staging layer for SEPE contracts data - standardized and cleaned with municipality code completion'
  )
}}

-- Create a municipality name to code mapping from records that have both
with municipality_code_mapping as (
    select distinct
        upper(trim(municipality_name)) as municipality_name_clean,
        municipality_code
    from {{ source('raw', 'raw_sepe_contracts') }}
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
            c.municipality_code,
            ml.municipality_code
        ) as municipality_code,
        c.municipality_name,
        
        -- Track whether municipality code was completed
        case 
            when c.municipality_code is null and ml.municipality_code is not null then true
            else false
        end as municipality_code_completed,
        
        -- Get province_code by joining with provinces mapping
        p.province_code,
        upper(trim(c.province)) as province_name,
        
        -- Date dimensions
        year as data_year,
        month as data_month,
        make_date(year::integer, month::integer, 1) as reporting_date,
        
        -- Total contracts metrics (validated)
        case 
            when total_contracts >= 0 then total_contracts::integer
            else null 
        end as total_contracts,
        
        -- Gender and contract type breakdown metrics (validated)
        case when men_indefinite_initial >= 0 then men_indefinite_initial::integer else null end as men_indefinite_initial,
        case when men_temporary_initial >= 0 then men_temporary_initial::integer else null end as men_temporary_initial,
        case when men_indefinite_conversion >= 0 then men_indefinite_conversion::integer else null end as men_indefinite_conversion,
        case when women_indefinite_initial >= 0 then women_indefinite_initial::integer else null end as women_indefinite_initial,
        case when women_temporary_initial >= 0 then women_temporary_initial::integer else null end as women_temporary_initial,
        case when women_indefinite_conversion >= 0 then women_indefinite_conversion::integer else null end as women_indefinite_conversion,
        
        -- Calculated gender totals
        (coalesce(men_indefinite_initial, 0) + coalesce(men_temporary_initial, 0) + coalesce(men_indefinite_conversion, 0))::integer as total_men_contracts,
        (coalesce(women_indefinite_initial, 0) + coalesce(women_temporary_initial, 0) + coalesce(women_indefinite_conversion, 0))::integer as total_women_contracts,
        
        -- Calculated contract type totals
        (coalesce(men_indefinite_initial, 0) + coalesce(women_indefinite_initial, 0))::integer as total_indefinite_initial,
        (coalesce(men_temporary_initial, 0) + coalesce(women_temporary_initial, 0))::integer as total_temporary_initial,
        (coalesce(men_indefinite_conversion, 0) + coalesce(women_indefinite_conversion, 0))::integer as total_indefinite_conversion,
        
        -- Sector breakdown metrics (validated)
        case when agriculture_sector >= 0 then agriculture_sector::integer else null end as agriculture_sector,
        case when industry_sector >= 0 then industry_sector::integer else null end as industry_sector,
        case when construction_sector >= 0 then construction_sector::integer else null end as construction_sector,
        case when services_sector >= 0 then services_sector::integer else null end as services_sector,
        
        -- Data lineage
        source_file,
        data_source,
        data_source_full,
        data_category,
        ingestion_timestamp
        
    from {{ source('raw', 'raw_sepe_contracts') }} c
    left join municipality_lookup ml
        on upper(trim(c.municipality_name)) = ml.municipality_name_clean
    left join {{ ref('provinces_autonomous_communities') }} p
        on upper(trim(c.province)) = upper(trim(p.province_name))
    
    -- Data quality filters
    where c.municipality_name is not null
      and c.municipality_name != ''
      and c.year between 2005 and 2025
      and c.month between 1 and 12
      and c.total_contracts is not null
      and c.total_contracts >= 0
),

final as (
    select 
        *,
        -- Additional calculated fields for analysis
        case 
            when total_contracts > 0 then round((total_men_contracts::numeric / total_contracts::numeric) * 100, 2)
            else null 
        end as men_contracts_percentage,
        
        case 
            when total_contracts > 0 then round((total_women_contracts::numeric / total_contracts::numeric) * 100, 2)
            else null 
        end as women_contracts_percentage,
        
        -- Contract type percentages
        case 
            when total_contracts > 0 then round((total_indefinite_initial::numeric / total_contracts::numeric) * 100, 2)
            else null 
        end as indefinite_initial_percentage,
        
        case 
            when total_contracts > 0 then round((total_temporary_initial::numeric / total_contracts::numeric) * 100, 2)
            else null 
        end as temporary_initial_percentage,
        
        case 
            when total_contracts > 0 then round((total_indefinite_conversion::numeric / total_contracts::numeric) * 100, 2)
            else null 
        end as indefinite_conversion_percentage,
        
        -- Contract stability indicator (higher is more stable employment)
        case 
            when total_contracts > 0 then round(((total_indefinite_initial + total_indefinite_conversion)::numeric / total_contracts::numeric) * 100, 2)
            else null 
        end as permanent_contracts_percentage
        
    from source_data
)

select * from final