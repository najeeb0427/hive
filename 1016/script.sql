drop table test2;
create table test2(id int,month string,Total_Production int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
;
load data local inpath 'data.txt' into table test2;

select t.id, t.month,t.Total_Production,case when t.RowNumber < M.start_row - 1 then 0 when V.RowNumber is null then 1 else V.RowNumber-M.start_row end as normalized_month,case when V.RowNumber is null then 1 else V.RowNumber+1 end as Expected_Month from
(SELECT id,month,Total_Production,ROW_NUMBER() OVER (partition by id Order by from_unixtime(unix_timestamp(month,'yyyy-dd-MM'),'yyyy-MM-dd')) AS RowNumber FROM test2) t 
 left join 
(SELECT id,month,Total_Production,ROW_NUMBER() OVER (partition by id Order by from_unixtime(unix_timestamp(month,'yyyy-dd-MM'),'yyyy-MM-dd')) AS RowNumber FROM test2) V
on t.RowNumber = V.RowNumber + 1 and t.id = V.id
join
(
	select id,min(case when Total_Production > 15000 then RowNumber else 2147483647 end) as start_row from  
	(SELECT id,month,Total_Production,ROW_NUMBER() OVER (partition by id Order by from_unixtime(unix_timestamp(month,'yyyy-dd-MM'),'yyyy-MM-dd')) AS RowNumber FROM test2) t group by id
) M
on t.id = M.id
;


