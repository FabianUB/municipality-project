-- Municipality Size Distribution Report
-- Usage: Shows distribution of municipalities by population size categories
-- Parameters: Change year as needed

SELECT 
    municipality_size_category,
    COUNT(*) as municipality_count,
    SUM(population_total) as total_population,
    ROUND(AVG(population_total), 0) as avg_population,
    ROUND(
        (COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER ()) * 100, 2
    ) as percentage_of_municipalities,
    ROUND(
        (SUM(population_total)::NUMERIC / SUM(SUM(population_total)) OVER ()) * 100, 2
    ) as percentage_of_population
FROM {{ ref('mart_population_summary') }}
WHERE data_year = 2024  -- Change year as needed
GROUP BY municipality_size_category
ORDER BY 
    CASE municipality_size_category
        WHEN 'Large (100k+)' THEN 1
        WHEN 'Medium (20k-100k)' THEN 2  
        WHEN 'Small (5k-20k)' THEN 3
        WHEN 'Very Small (1k-5k)' THEN 4
        WHEN 'Micro (<1k)' THEN 5
    END;