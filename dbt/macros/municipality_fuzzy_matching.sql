{% macro normalize_municipality_name(municipality_name_column) %}
  -- Comprehensive text normalization for Spanish municipality names
  upper(
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        -- Remove accents and special characters
                        translate(
                          upper(trim({{ municipality_name_column }})),
                          'ÁÉÍÓÚÜÑÇ.,()-/',
                          'AEIOUUNC      '
                        ),
                        -- Standardize common abbreviations
                        '\bSTA\.?\s*', 'SANTA ', 'g'
                      ),
                      '\bSTO\.?\s*', 'SANTO ', 'g'
                    ),
                    '\bS\.?\s*', 'SAN ', 'g'
                  ),
                  '\bN\.?\s*SRA\.?\s*', 'NUESTRA SENORA ', 'g'
                ),
                '\bN\.?\s*S\.?\s*', 'NUESTRA SENORA ', 'g'
              ),
              -- Handle "DE LA", "DEL", etc.
              '\s+DE\s+LA\s+', ' DE LA ', 'g'
            ),
            '\s+DEL\s+', ' DEL ', 'g'
          ),
          '\s+DE\s+', ' DE ', 'g'
        ),
        -- Clean multiple spaces
        '\s+', ' ', 'g'
      ),
      -- Remove trailing/leading spaces after all replacements
      '^\s+|\s+$', '', 'g'
    )
  )
{% endmacro %}

{% macro get_fuzzy_municipality_matches(sepe_municipality_column, sepe_province_code_column, sepe_source_table, ine_reference_table='stg_ine_codes_data__municipalities') %}
  with fuzzy_matching_candidates as (
    select distinct
      sepe_data.municipality_name as sepe_municipality_name,
      sepe_data.province_code as sepe_province_code,
      ine.municipality_code as ine_municipality_code,
      ine.municipality_name as ine_municipality_name,
      
      -- Normalized names for comparison
      {{ normalize_municipality_name('sepe_data.municipality_name') }} as sepe_normalized,
      {{ normalize_municipality_name('ine.municipality_name') }} as ine_normalized,
      
      -- Matching methods and confidence scores
      case
        -- Exact normalized match (highest confidence)
        when {{ normalize_municipality_name('sepe_data.municipality_name') }} = {{ normalize_municipality_name('ine.municipality_name') }}
          then 'exact_normalized'
        
        -- Substring matching (medium confidence)
        when {{ normalize_municipality_name('ine.municipality_name') }} like '%' || {{ normalize_municipality_name('sepe_data.municipality_name') }} || '%'
          and length({{ normalize_municipality_name('sepe_data.municipality_name') }}) >= 4
          then 'substring_match'
        
        -- Reverse substring (SEPE name contains INE name)
        when {{ normalize_municipality_name('sepe_data.municipality_name') }} like '%' || {{ normalize_municipality_name('ine.municipality_name') }} || '%'
          and length({{ normalize_municipality_name('ine.municipality_name') }}) >= 4
          then 'reverse_substring'
        
        -- Simple word matching for abbreviations (fallback)
        when position({{ normalize_municipality_name('sepe_data.municipality_name') }} in {{ normalize_municipality_name('ine.municipality_name') }}) > 0
          and length({{ normalize_municipality_name('sepe_data.municipality_name') }}) >= 6
          then 'word_match'
        
        else null
      end as match_method,
      
      -- Confidence score (0-100)
      case
        when {{ normalize_municipality_name('sepe_data.municipality_name') }} = {{ normalize_municipality_name('ine.municipality_name') }}
          then 100
        when {{ normalize_municipality_name('ine.municipality_name') }} like '%' || {{ normalize_municipality_name('sepe_data.municipality_name') }} || '%'
          and length({{ normalize_municipality_name('sepe_data.municipality_name') }}) >= 4
          then 85
        when {{ normalize_municipality_name('sepe_data.municipality_name') }} like '%' || {{ normalize_municipality_name('ine.municipality_name') }} || '%'
          and length({{ normalize_municipality_name('ine.municipality_name') }}) >= 4
          then 80
        when position({{ normalize_municipality_name('sepe_data.municipality_name') }} in {{ normalize_municipality_name('ine.municipality_name') }}) > 0
          and length({{ normalize_municipality_name('sepe_data.municipality_name') }}) >= 6
          then 75
        else 0
      end as confidence_score
      
    from (
      select distinct 
        upper(trim(municipality_name)) as municipality_name, 
        province_code 
      from {{ sepe_source_table }}
      where municipality_code is null or municipality_code = 0
    ) sepe_data
    join {{ ref(ine_reference_table) }} ine
      on ine.province_code = sepe_data.province_code
    where sepe_data.province_code is not null
      and sepe_data.municipality_name is not null
      and sepe_data.municipality_name != ''
  ),
  
  -- Rank matches by confidence and prefer exact matches
  ranked_matches as (
    select 
      *,
      row_number() over (
        partition by sepe_municipality_name, sepe_province_code 
        order by confidence_score desc, 
                 case when match_method = 'exact_normalized' then 1 else 2 end,
                 length(ine_municipality_name) asc  -- Prefer shorter names for ties
      ) as match_rank
    from fuzzy_matching_candidates
    where match_method is not null
      and confidence_score >= 70  -- Minimum confidence threshold
  )
  
  select * from ranked_matches where match_rank = 1
{% endmacro %}

{% macro join_fuzzy_municipality_matching(sepe_municipality_column, sepe_province_code_column, sepe_source_table, ine_reference_table='stg_ine_codes_data__municipalities') %}
  left join ({{ get_fuzzy_municipality_matches(sepe_municipality_column, sepe_province_code_column, sepe_source_table, ine_reference_table) }}) fuzzy_match
    on upper(trim({{ sepe_municipality_column }})) = fuzzy_match.sepe_municipality_name
    and {{ sepe_province_code_column }} = fuzzy_match.sepe_province_code
{% endmacro %}