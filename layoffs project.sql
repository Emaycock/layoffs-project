SELECT * FROM layoffs_staging;

select *, 
Row_Number() OVER( Partition by company, industry, total_laid_off, percentage_laid_off, 'date' ) AS row_num
from layoffs_staging;


WITH duplicate_cte AS ( 
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY company, location,industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions ) AS row_num
    FROM layoffs_staging
    )
select * from duplicate_cte 
where row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;


insert into layoffs_staging2
SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY company, location,industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions ) AS row_num
    FROM layoffs_staging;
    
select * from layoffs_staging2
where row_num> 1;


SET SQL_SAFE_UPDATES = 0;


DELETE from layoffs_staging2
where row_num> 1;


select company, TRIM(company)
from layoffs_staging;

Update layoffs_staging2
set company = TRIM(company);


select distinct country
from layoffs_staging2;


Update layoffs_staging2
set country = Trim(trailing '.' from country)
where country like 'United States%';


Update layoffs_staging2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

alter table  layoffs_staging2
Modify column `date` DATE;

select * 
from layoffs_staging2
where total_laid_off is NULL
and percentage_laid_off is NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT MAX(total_laid_off)
FROM layoffs_staging2;

SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


SELECT 
    country, total_laid_off
FROM
    layoffs_staging
ORDER BY 2 DESC
LIMIT 5;


SELECT 
    company, total_laid_off
FROM
    layoffs_staging
ORDER BY 2 DESC
LIMIT 5;

SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;


WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;


WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

