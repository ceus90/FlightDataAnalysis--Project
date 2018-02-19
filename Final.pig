A = load 's3://flightdataanalysis/Input' using PigStorage(',') as
(YEAR:int,	MONTH:int,	DAY_OF_MONTH:int,	DAY_OF_WEEK:int,	FL_DATE:chararray,	UNIQUE_CARRIER:chararray,	TAIL_NUM:chararray,	FL_NUM:int,	ORIGIN_AIRPORT_ID:int,	ORIGIN:chararray,	ORIGIN_STATE_ABR:chararray,	DEST_AIRPORT_ID:int,	DEST:chararray,	DEST_STATE_ABR:chararray,	CRS_DEP_TIME:int,	DEP_TIME:int,	DEP_DELAY:int,	DEP_DELAY_NEW:int,	DEP_DEL15:int,	DEP_DELAY_GROUP:int,	TAXI_OUT:int,	WHEELS_OFF:int,	WHEELS_ON:int,	TAXI_IN:int,	CRS_ARR_TIME:int,	ARR_TIME:int,	ARR_DELAY:int,	ARR_DELAY_NEW:int,	ARR_DEL15:int,	ARR_DELAY_GROUP:int,	CANCELLED:int,	CANCELLATION_CODE:chararray,	DIVERTED:int,	CRS_ELAPSED_TIME:int,	ACTUAL_ELAPSED_TIME:int,	AIR_TIME:int,	FLIGHTS:int,	DISTANCE:int,	DISTANCE_GROUP:int,	CARRIER_DELAY:int,	WEATHER_DELAY:int,	NAS_DELAY:int,	SECURITY_DELAY:int,	LATE_AIRCRAFT_DELAY:int);

B = filter A by ARR_DELAY < 15 and DEP_DELAY < 15;
C = foreach B generate YEAR, MONTH, DAY_OF_MONTH, FL_NUM, ARR_DELAY, DEP_DELAY;
D = group B all;
E = foreach D generate COUNT(B);
store C into 's3://flightdataanalysis/ontimeflights';
store E into 's3://flightdataanalysis/ontimeoutput';

F = group A by UNIQUE_CARRIER;
G = foreach F 
{
	H = COUNT(A);
	B = filter A by ARR_DELAY < 15 and DEP_DELAY < 15;
	I = COUNT(B);
	generate group, I,H,(float)I/H;
};
store G into 's3://flightdataanalysis/ontimeflightscarrier';

J = foreach A generate ORIGIN as from, DEST as to;
K = group J by (from,to);
L = foreach K generate group, COUNT(J) as fcnt;
M = order L by fcnt desc;
N = limit M 10;
store N into 's3://flightdataanalysis/toptenbusy';

incoming = foreach A generate MONTH as m, DEST as d;
g_incoming = group incoming by (m,d);
cnt_incoming = foreach g_incoming generate FLATTEN(group), COUNT(incoming) as cnt;
g_cnt_incoming = group cnt_incoming by m;
top_incoming = foreach g_cnt_incoming
{
    result = TOP(20, 2, cnt_incoming); 
    generate FLATTEN(result);
}
store top_incoming into 's3://flightdataanalysis/top_incoming_traffic_monthly';

outgoing = foreach A generate MONTH as m, ORIGIN as o;
g_outgoing = group outgoing by (m,o);
cnt_outgoing = foreach g_outgoing generate FLATTEN(group), COUNT(outgoing) as cnt;
g_cnt_outgoing = group cnt_outgoing by m;
top_outgoing = foreach g_cnt_outgoing
{
    result = TOP(20, 2, cnt_outgoing); 
    generate FLATTEN(result);
}
store top_incoming into 's3://flightdataanalysis/top_outgoing_traffic_monthly';

traffic = UNION cnt_incoming, cnt_outgoing;
g_traffic = group traffic by (m,d);
tot_traffic = foreach g_traffic generate FLATTEN(group) as (m, CANCELLATION_CODE), SUM(traffic.cnt) as tot; 
tot_per_month = group tot_traffic by m;
top_per_month = foreach tot_per_month
{
    result = TOP(20, 2, tot_traffic); 
    generate FLATTEN(result) as (MONTH, DEST, traffic);
}
store top_per_month into 's3://flightdataanalysis/top_per_month_traffic';

c_cancel = foreach A generate YEAR, MONTH, FL_NUM, CANCELLED, CANCELLATION_CODE;
filter_cancelled = filter c_cancel by CANCELLED == 1 AND CANCELLATION_CODE =='c_cancel';
g_filter_cancelled = group filter_cancelled by MONTH;
cnt_cancelled = foreach g_filter_cancelled generate group, COUNT(filter_cancelled.CANCELLED);
max_cancelled = limit cnt_cancelled 10;
store max_cancelled into 's3://flightdataanalysis/max_cancelled';

diverted_info = foreach A generate YEAR,ORIGIN,DEST,DIVERTED;
diverted_true = FILTER diverted_info BY (ORIGIN is not null) AND (DEST is not null) AND (DIVERTED == 1);
g_d_true = GROUP diverted_true by (ORIGIN,DEST);
cnt_gdtrue = FOREACH g_d_true generate group, COUNT(diverted_true.DIVERTED);
most_diverted = limit cnt_gdtrue 10;
store most_diverted into 's3://flightdataanalysis/most_diverted';


distance_list = foreach A generate UNIQUE_CARRIER, ORIGIN, DEST, DISTANCE;
distance_group = order distance_list by DISTANCE desc; 
result = limit by distance_group 20;
store result into 's3://flightdataanalysis/max_distance';