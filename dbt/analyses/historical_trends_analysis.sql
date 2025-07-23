-- Historical Population Trends Analysis
-- Usage: Shows population evolution over time for specific municipalities or provinces
-- Parameters: Adjust municipality/province filters and year range

SELECT 
    municipality_name,
    province_name,
    data_year,
    population_total,
    population_change,
    population_change_pct,
    -- Calculate 5-year moving average
    AVG(population_total) OVER (
        PARTITION BY municipality_name 
        ORDER BY data_year 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    )::INTEGER as five_year_avg_population
FROM {{ ref('mart_population_summary') }}
WHERE municipality_name IN ('Madrid', 'Barcelona', 'València', 'Sevilla')  -- Focus on major cities
  AND data_year BETWEEN 2010 AND 2024  -- Adjust year range
ORDER BY municipality_name, data_year;