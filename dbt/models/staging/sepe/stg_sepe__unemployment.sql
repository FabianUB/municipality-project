{{
  config(
    materialized='view',
    description='Staging layer for SEPE unemployment data - standardized and cleaned with municipality code completion'
  )
}}

-- Enhanced municipality lookup with province context using reusable macro
with municipality_lookup as (
    {{ get_enhanced_municipality_lookup('raw_sepe_unemployment', province_context=true) }}
),

source_data as (
    select 
        -- Geographic identifiers with enhanced municipality code completion
        coalesce(
            case when u.municipality_code != 0 then u.municipality_code else null end,
            ml.municipality_code,
            ine_muni.municipality_code  -- Additional lookup from INE reference
        ) as municipality_code,
        u.municipality_name,
        
        -- Track municipality code completion method
        case 
            when u.municipality_code is not null and u.municipality_code != 0 then 'original'
            when ml.municipality_code is not null then 'sepe_lookup'
            when ine_muni.municipality_code is not null then 'ine_reference'
            else 'not_found'
        end as municipality_code_source,
        
        -- Get province_code with enhanced mapping using macro
        {{ get_enhanced_province_code('u.province') }} as province_code,
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
        and upper(trim(u.province)) = ml.province_name_clean
    
    -- Enhanced province mapping using macro
    {{ join_enhanced_province_mapping('u.province') }}
    
    -- Additional municipality lookup from INE reference data (with province context)
    left join {{ ref('stg_ine_codes_data__municipalities') }} ine_muni
        on upper(trim(u.municipality_name)) = upper(trim(ine_muni.municipality_name))
        and coalesce(p_direct.province_code, p_mapped.province_code) = ine_muni.province_code
    
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