/*
Covid 19 Data Exploration 
Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types
*/


select *
from [Portfolio Project]..Covid_Deaths$
where continent is not null
order by 3,4

--select *
--from [Portfolio Project]..CovidVaccines
--where continent is not null
--order by 3,4


--Select Data that we are going to be starting with

select location, date, total_cases, new_cases, total_deaths, population
from [Portfolio Project]..Covid_Deaths$
order by 1,2


-- Looking at Total cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in your country

select location, date, total_cases, new_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
from [Portfolio Project]..Covid_Deaths$
where location = 'Nigeria'
order by 1,2


-- Looking at Total cases vs Population
--Shows what percentage of population infected with Covid

select location, date, total_cases, population, (total_cases/population)*100 as PercentPopulationInfected
from [Portfolio Project]..Covid_Deaths$
where location = 'Nigeria'
order by 1,2


-- Countries with Highest Infection Rate compared to population

select location, population, MAX(total_cases) as HighestInfectionCount, MAX((total_cases/population))*100 as PercentPopulationInfected
from [Portfolio Project]..Covid_Deaths$
Group by location,population
order by PercentPopulationInfected Desc


-- Countries with the Highest Death Count per population

select location, MAX(cast(total_deaths as int)) as TotalDeathCount
from [Portfolio Project]..Covid_Deaths$
where continent is not null
Group by location
order by TotalDeathCount Desc

-- BREAKING THINGS DOWN BY CONTINENT

-- Showing continents with the Highest Death Count per population

select continent, MAX(cast(total_deaths as int)) as TotalDeathCount
from [Portfolio Project]..Covid_Deaths$
where continent is not null
Group by continent
order by TotalDeathCount Desc

-- GLOBAL NUMBERS

select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(new_cases)*100 as DeathPercentage
from [Portfolio Project]..Covid_Deaths$
where continent is not null 
order by 1,2


-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine

Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3


-- Using CTE to perform Calculation on Partition By in previous query

with PopvsVac (continent, location, Date, Population, new_vaccinations, RollingPeopleVaccinated)
as
(
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(Convert(int,vac.new_vaccinations)) OVER (Partition by dea.location Order by dea.location
, dea.Date) as RollingPeopleVaccinated
from [Portfolio Project]..Covid_Deaths$ dea
Join [Portfolio Project]..CovidVaccines vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
)
select *, (RollingPeopleVaccinated/Population)*100
from PopvsVac


-- Using TEMP TABLE to perform Calculation on Partition By in previous query

Drop Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)

Insert into #PercentPopulationVaccinated
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(Convert(int,vac.new_vaccinations)) OVER (Partition by dea.location Order by dea.location
, dea.Date) as RollingPeopleVaccinated
from [Portfolio Project]..Covid_Deaths$ dea
Join [Portfolio Project]..CovidVaccines vac
	On dea.location = vac.location
	and dea.date = vac.date


-- Creating View to store data for later visualizations

create view PercentagePopulationVaccinated as
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(Convert(int,vac.new_vaccinations)) OVER (Partition by dea.location Order by dea.location
, dea.Date) as RollingPeopleVaccinated
from [Portfolio Project]..Covid_Deaths$ dea
Join [Portfolio Project]..CovidVaccines vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
