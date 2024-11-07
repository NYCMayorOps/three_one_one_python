SELECT *
FROM [dbo].[SR] as sr
WHERE sr.SR_NUMBER NOT IN (
						   SELECT sr_number
						   FROM [dbo].[ThreeOneOneGeom]
						   )
AND sr.LAT is not null;