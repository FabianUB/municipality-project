{% macro get_province_name_mapping() %}
  select province_name_sepe, province_name_ine from (
    values 
    -- Basque provinces (SEPE uses Spanish, INE uses bilingual)
    ('ALAVA', 'Araba/Álava'),
    ('ARABA', 'Araba/Álava'),  -- Alternative format
    ('VIZCAYA', 'Bizkaia'),
    ('GUIPUZCOA', 'Gipuzkoa'),
    ('GIPUZCOA', 'Gipuzkoa'),  -- Alternative format
    ('GUIPÚZCOA', 'Gipuzkoa'), -- With accent
    ('GUIPUZKOA', 'Gipuzkoa'),  -- No accent variation
    
    -- Catalonia (SEPE uses Spanish, INE uses Catalan)
    ('GERONA', 'Girona'),
    ('LERIDA', 'Lleida'),
    
    -- Valencia (SEPE uses Spanish, INE uses bilingual)
    ('ALICANTE', 'Alicante/Alacant'),
    ('CASTELLON', 'Castellón/Castelló'),
    ('VALENCIA', 'Valencia/València'),
    
    -- Balearics (SEPE uses old name)
    ('BALEARES', 'Balears, Illes'),
    ('ILLES BALEARS', 'Balears, Illes'),  -- Alternative format
    ('ILLES BALEARES', 'Balears, Illes'), -- Alternative format
    
    -- Galicia (SEPE uses Spanish, INE uses Galician)
    ('LA CORUÑA', 'Coruña, A'),
    ('CORUÑA', 'Coruña, A'),  -- Alternative format
    ('ORENSE', 'Ourense'),
    ('PONTEVEDRA', 'Pontevedra'),
    ('LUGO', 'Lugo'),
    
    -- Other common variations
    ('CORDOBA', 'Córdoba'),
    ('CADIZ', 'Cádiz'),
    ('MALAGA', 'Málaga'),
    ('JAEN', 'Jaén'),
    ('ALMERIA', 'Almería'),
    ('AVILA', 'Ávila'),
    ('CACERES', 'Cáceres'),
    ('LEON', 'León'),
    ('RIOJA (LA)', 'Rioja, La'),
    ('LA RIOJA', 'Rioja, La'),  -- Alternative format
    
    -- Asturias (Oviedo is the province name, Asturias is CCAA name but sometimes used)
    ('ASTURIAS', 'Asturias'),
    ('OVIEDO', 'Asturias'),
    
    -- Canary Islands
    ('LAS PALMAS', 'Palmas, Las'),
    ('SANTA CRUZ DE TENERIFE', 'Santa Cruz de Tenerife'),
    ('SANTA CRUZ TENERIFE', 'Santa Cruz de Tenerife'), -- Alternative format
    ('TENERIFE', 'Santa Cruz de Tenerife') -- Alternative format
    
  ) as mapping(province_name_sepe, province_name_ine)
{% endmacro %}

{% macro get_enhanced_province_code(sepe_province_column, ine_reference_table='stg_ine_codes_data__provinces') %}
  coalesce(
    p_direct.province_code, 
    p_mapped.province_code
  )
{% endmacro %}

{% macro join_enhanced_province_mapping(sepe_province_column, ine_reference_table='stg_ine_codes_data__provinces') %}
  -- Direct province name match
  left join {{ ref(ine_reference_table) }} p_direct
    on upper(trim({{ sepe_province_column }})) = upper(trim(p_direct.province_name))
  
  -- Province name mapping for SEPE variations
  left join ({{ get_province_name_mapping() }}) pnm
    on upper(trim({{ sepe_province_column }})) = upper(trim(pnm.province_name_sepe))
  left join {{ ref(ine_reference_table) }} p_mapped
    on upper(trim(pnm.province_name_ine)) = upper(trim(p_mapped.province_name))
{% endmacro %}

{% macro get_enhanced_municipality_lookup(sepe_table, province_context=true) %}
  with municipality_code_mapping as (
    select distinct
      upper(trim(municipality_name)) as municipality_name_clean,
      municipality_code,
      {% if province_context %}
      upper(trim(province)) as province_name_clean
      {% endif %}
    from {{ source('raw', sepe_table) }}
    where municipality_code is not null 
      and municipality_name is not null
      and municipality_name != ''
  ),

  -- Validate the mapping - ensure one-to-one relationship
  municipality_mapping_validated as (
    select 
      municipality_name_clean,
      municipality_code,
      {% if province_context %}
      province_name_clean,
      count(*) over (
        partition by municipality_name_clean, province_name_clean
      ) as codes_per_name_province,
      {% endif %}
      count(*) over (partition by municipality_name_clean) as codes_per_name,
      count(*) over (partition by municipality_code) as names_per_code
    from municipality_code_mapping
  ),

  -- Final mapping table
  municipality_lookup as (
    select distinct
      municipality_name_clean,
      municipality_code
      {% if province_context %}
      , province_name_clean
      {% endif %}
    from municipality_mapping_validated
    where codes_per_name = 1 and names_per_code = 1
      {% if province_context %}
      and codes_per_name_province = 1
      {% endif %}
  )
  
  select * from municipality_lookup
{% endmacro %}