-- 1. STRATEGIC ANALYSIS: Evolution of US Incendiary Bombing on Japan (1945)
WITH MonthlyStats AS (
    SELECT 
        TO_CHAR(mission_date, 'YYYY-MM') AS mission_month,
        SUM(high_explosives_weight_tons) AS total_he,
        SUM(incendiary_devices_weight_tons) AS total_incendiary,
        SUM(total_weight_tons) AS total_weight
    FROM ww2_missions
    WHERE target_country = 'JAPAN' 
      AND country = 'USA' 
      AND EXTRACT(YEAR FROM mission_date) = 1945
    GROUP BY TO_CHAR(mission_date, 'YYYY-MM')
)
SELECT 
    mission_month,
    ROUND(CAST(total_he AS numeric), 2) AS he_tons,
    ROUND(CAST(total_incendiary AS numeric), 2) AS incendiary_tons,
    -- Prevent Divide-by-Zero error using NULLIF
    ROUND(CAST((total_incendiary / NULLIF(total_weight, 0)) * 100 AS numeric), 2) AS incendiary_percentage
FROM MonthlyStats
ORDER BY mission_month;

-- 2. TIME-SERIES: Cumulative Trend (Running Total) of the USA War Effort
-- NOTE: The focus was shifted from 'GREAT BRITAIN' to 'USA' because the source 
-- dataset contains significant gaps for RAF missions between 1942 and 1945. 
-- Using USA data provides a consistent trend for demonstrating Window Functions.

WITH MonthlyUSA AS (
    SELECT 
        TO_CHAR(mission_date, 'YYYY-MM') AS mission_month,
        SUM(total_weight_tons) AS monthly_tons
    FROM ww2_missions
    WHERE country = 'USA' AND mission_date IS NOT NULL
    GROUP BY TO_CHAR(mission_date, 'YYYY-MM')
)
SELECT 
    mission_month,
    ROUND(CAST(monthly_tons AS numeric), 2) AS tons_this_month,
    -- Calculate running total using Window Function to show war effort escalation
    ROUND(CAST(SUM(monthly_tons) OVER (ORDER BY mission_month) AS numeric), 2) AS cumulative_tons_dropped
FROM MonthlyUSA
ORDER BY mission_month;

-- 3. PARTITIONED RANKING: Primary Target (Top 1) Year by Year
WITH RankedTargets AS (
    SELECT 
        EXTRACT(YEAR FROM mission_date) AS mission_year,
        target_country,
        SUM(total_weight_tons) AS total_tons,
        -- Assign rank 1 to the heaviest bombed country per year
        ROW_NUMBER() OVER(
            PARTITION BY EXTRACT(YEAR FROM mission_date) 
            ORDER BY SUM(total_weight_tons) DESC
        ) as rank
    FROM ww2_missions
    WHERE mission_date IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM mission_date), target_country
)
SELECT 
    mission_year, 
    target_country AS primary_target, 
    ROUND(CAST(total_tons AS numeric), 2) AS tons_dropped
FROM RankedTargets
WHERE rank = 1
ORDER BY mission_year;
