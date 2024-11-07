/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (2000) [sr_number]
      ,[Expr1]
      ,[geoid]
      ,[type]
      ,[CREATED_DATE]
      ,[LAT]
      ,[LON]
  FROM [311SR].[dbo].[sr_to_geom_join_cd_48hrs]

  SELECT COUNT(sr_number)
  FROM [311SR].[dbo].[sr_to_geom_join_cd_48hrs]

  SELECT MIN(CREATED_DATE)
  FROM sr_to_geom_join_cd_48hrs

  SELECT MAX(CREATED_DATE)
  FROM sr_to_geom_join_cd_48hrs