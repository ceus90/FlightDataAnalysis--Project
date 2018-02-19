A = load 's3://flightdataanalysis/CSVFiles/*.csv' using PigStorage(',') as
(year:int,month:int,date:int,day:int,deptime:int,crsdeptime:int,arrtime:int,crsarrtime:int,uniquecarrier:chararray,fnum:int,tnum:int,etime:int,crsetime:int,atime:int,arrdelay:int,depdelay:int,org:chararray,
dest:chararray,dist:int,tin:int,tout:int,cancelled:int,cancelcode:chararray,diverted:int,cardelay:int,weatherdelay:int,nasdelay:int,secdelay:int,lateairdelay:int);

B = foreach A generate org as from, dest as to;
C = group B by (from,to);
D = foreach C generate group, COUNT(B) as fcnt;
E = order D by fcnt desc;

F = limit E 1;

store F into 's3://flightdataanalysis/output2c';