/****** Script for SelectTopNRows command from SSMS  ******/

select sum(sr_count) AS 'post_join_sr_count'
FROM [311SR].[dbo].[agg_1d_ago]

select count(*) as 'count srs'
FROM dbo.sr_to_geom_join_cd_1d_ago

SELECT COUNT(DISTINCT sr_number) as 'post_join_1d_ago'
FROM dbo.sr_to_geom_join_cd_1d_ago


SELECT COUNT(*) AS 'actual_sr'
FROM SR
WHERE        (dbo.SR.CREATED_DATE > DATEADD(DAY, - 2, CAST(GETDATE() AS date)))
 AND (dbo.SR.CREATED_DATE < DATEADD(DAY, - 1, CAST(GETDATE() AS date)))


SELECT TOP 1000 *
FROM SR
WHERE        (dbo.SR.CREATED_DATE > DATEADD(DAY, - 2, CAST(GETDATE() AS date)))
 AND (dbo.SR.CREATED_DATE < DATEADD(DAY, - 1, CAST(GETDATE() AS date)))