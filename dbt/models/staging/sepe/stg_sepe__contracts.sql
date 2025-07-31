{{
  config(
    materialized='view',
    description='Staging layer for SEPE contracts data - standardized and cleaned with municipality code completion'
  )
}}

-- Enhanced municipality lookup with province context using reusable macro
with municipality_lookup as (
    {{ get_enhanced_municipality_lookup('raw_sepe_contracts', province_context=true) }}
),

source_data as (
    select 
        -- Geographic identifiers with enhanced municipality code completion (including fuzzy matching)
        coalesce(
            case when c.municipality_code != 0 then c.municipality_code else null end,
            ml.municipality_code,
            ine_muni.municipality_code,  -- INE exact reference lookup
            fuzzy_match.ine_municipality_code  -- NEW: Fuzzy matching lookup
        ) as municipality_code,
        c.municipality_name,
        
        -- Track municipality code completion method with fuzzy matching
        case 
            when c.municipality_code is not null and c.municipality_code != 0 then 'original'
            when ml.municipality_code is not null then 'sepe_lookup'
            when ine_muni.municipality_code is not null then 'ine_reference'
            when fuzzy_match.ine_municipality_code is not null then 'fuzzy_match'
            else 'not_found'
        end as municipality_code_source,
        
        -- NEW: Fuzzy matching metadata
        fuzzy_match.match_method,
        fuzzy_match.confidence_score,
        fuzzy_match.ine_municipality_name as fuzzy_matched_name,
        
        -- Get province_code with enhanced mapping using macro
        {{ get_enhanced_province_code('c.province') }} as province_code,
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
        and upper(trim(c.province)) = ml.province_name_clean
    
    -- Enhanced province mapping using macro
    {{ join_enhanced_province_mapping('c.province') }}
    
    -- Additional municipality lookup from INE reference data (with province context)
    left join {{ ref('stg_ine_codes_data__municipalities') }} ine_muni
        on upper(trim(c.municipality_name)) = upper(trim(ine_muni.municipality_name))
        and coalesce(p_direct.province_code, p_mapped.province_code) = ine_muni.province_code
    
    -- NEW: Basic fuzzy municipality matching (exact normalized match only for now)
    left join (
        select distinct
            {{ normalize_municipality_name('raw.municipality_name') }} as sepe_normalized_name,
            upper(trim(raw.municipality_name)) as sepe_municipality_name,
            upper(trim(raw.province)) as sepe_province_name,
            ine.municipality_code as ine_municipality_code,
            ine.municipality_name as ine_municipality_name,
            'fuzzy_normalized' as match_method,
            95 as confidence_score
        from {{ source('raw', 'raw_sepe_contracts') }} raw
        join {{ ref('stg_ine_codes_data__municipalities') }} ine
            on {{ normalize_municipality_name('raw.municipality_name') }} = {{ normalize_municipality_name('ine.municipality_name') }}
        where (raw.municipality_code is null or raw.municipality_code = 0)
            and raw.municipality_name is not null
            and raw.municipality_name != ''
    ) fuzzy_match
        on upper(trim(c.municipality_name)) = fuzzy_match.sepe_municipality_name
    
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