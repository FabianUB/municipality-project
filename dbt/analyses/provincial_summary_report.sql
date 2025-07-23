-- Provincial Summary Statistics Report
-- Usage: Provides province-level demographic overview
-- Parameters: Change year as needed

SELECT 
    province_name,
    COUNT(DISTINCT municipality_name) as municipality_count,
    SUM(population_total) as total_population,
    ROUND(AVG(population_total), 0) as avg_municipality_population,
    MAX(population_total) as largest_municipality_pop,
    MIN(population_total) as smallest_municipality_pop,
    ROUND(
        (SUM(population_male)::NUMERIC / SUM(population_total)) * 100, 2
    ) as male_percentage,
    ROUND(
        (SUM(population_female)::NUMERIC / SUM(population_total)) * 100, 2
    ) as female_percentage
FROM {{ ref('mart_population_summary') }}
WHERE data_year = 2024  -- Change year as needed
GROUP BY province_name
ORDER BY total_population DESC;