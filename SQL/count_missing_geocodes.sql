SELECT COUNT(*)
FROM [dbo].SR 
LEFT JOIN dbo.ThreeOneOneGeom 
ON dbo.SR.SR_NUMBER = dbo.ThreeOneOneGeom.sr_number
WHERE dbo.SR.LAT IS NOT NULL AND ThreeOneOneGeom.type is null