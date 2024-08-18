use Layoffs;
create table layoff (company varchar(255),
location varchar(255), industry varchar(255), total_laid_off int, percentage_laid_off float,
date text,stage varchar(255), country varchar(255), funds_raised_millions int);
select * from layoff;

create table staging_layoff like layoff;

Insert into staging_layoff(select * from layoff);
select * from staging_layoff;

With duplicate_cte as (select *, row_number() over (Partition By company, location,industry,total_laid_off,
percentage_laid_off,date,
stage, country, funds_raised_millions) as row_num from staging_layoff)

Select * from duplicate_cte where row_num>1;

create table staging_layoff2 (company varchar(255),
location varchar(255), industry varchar(255), total_laid_off int, percentage_laid_off float,
date text,stage varchar(255), country varchar(255), funds_raised_millions int, row_num int);

select * from staging_layoff2;
Insert into staging_layoff2 
select *, row_number() over (Partition By company, location,industry,total_laid_off,
percentage_laid_off,date,
stage, country, funds_raised_millions) as row_num from staging_layoff;

delete from staging_layoff2 where row_num > 2;



#Standardizing data

select distinct(company) from staging_layoff2;
select company,trim(company) from staging_layoff2;
update staging_layoff2 set company = trim(company);

select * from staging_layoff2;

select distinct(industry) from staging_layoff2 order by 1;

select * from staging_layoff2 where industry like 'Crypto%';

update staging_layoff2 set industry = 'Crypto' where industry like 'Crypto%';

select distinct country from staging_layoff2 order by 1;
select distinct location from staging_layoff2 order by 1;
select country from staging_layoff2 where country like 'United States.';

update staging_layoff2 set country= trim(trailing '.' from country) where country like 'United States%';

select date, str_to_date(date,'%m/%d/%Y') from staging_layoff2;
alter table staging_layoff2 modify column date date;
update staging_layoff2 set date = str_to_date(date, '%m/%d/%Y');

select date from staging_layoff2;

Select * from staging_layoff2 where total_laid_off is NULL;

Select * from staging_layoff2 where industry is null or industry = '';

select t1.industry, t2.industry from staging_layoff2 t1 join staging_layoff2 t2
on t1.company = t2.company and t1.location = t2.location
where t1.industry is null or t1.industry = ''
and t2.industry is not null;

update staging_layoff2 t1 join staging_layoff2 t2 on t1.company =t2.company set t1.industry = t2.industry
where (t1.industry is null) and t2.industry is not null; 

update staging_layoff2 set industry = null where industry = '';

select * from staging_layoff2 where company ='Airbnb';


delete from staging_layoff2 where total_laid_off is null and percentage_laid_off is null;

select * from staging_layoff2;

alter table staging_layoff2 drop column row_num;


#Data Exploration

select max(total_laid_off), max(percentage_laid_off) from staging_layoff2;

select * from staging_layoff2 where percentage_laid_off =1 order by total_laid_off desc;

select * from staging_layoff2 where percentage_laid_off =1 order by funds_raised_millions desc;

select company, sum(total_laid_off) from staging_layoff2 group by company order by 2 desc;

select min(date),max(date) from staging_layoff2;

select industry, sum(total_laid_off) from staging_layoff2 group by industry order by 2 desc;

select country, sum(total_laid_off) from staging_layoff2 group by country order by 2 desc;

select year(date), sum(total_laid_off) from staging_layoff2 group by year(date) order by 2 desc;

select stage, sum(total_laid_off) from staging_layoff2 group by stage order by 2 desc;

select substring(date, 1,7) as month, sum(total_laid_off) from staging_layoff2 where substring(date,1,7) is not null
group by month order by 1 asc;

with rolling_total as
(select substring(date,1,7) as Month, sum(total_laid_off) as total_off from staging_layoff2
where substring(date,1,7) is not null group by month order by 1 asc)
select Month, total_off, sum(total_off) over(order by month) as rolling_total from rolling_total;

select company, year(date), sum(total_laid_off) from staging_layoff2 group by company, year(date) order by 3 DESC;

With company_year(company, years, total_laid_off) as 
(select company, year(date), sum(total_laid_off) from staging_layoff2 group by company, year(date)), Company_year_rank AS
(select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking from company_year 
where years is not null)
select * from Company_year_rank where ranking <=5;









