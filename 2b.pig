A = load 's3://flightdataanalysis/CSVFiles' using PigStorage(',') as
(year:int,month:int,date:int,day:int,deptime:int,crsdeptime:int,arrtime:int,crsarrtime:int,uniquecarrier:chararray,fnum:int,tnum:int,etime:int,crsetime:int,atime:int,arrdelay:int,depdelay:int,org:chararray,
dest:chararray,dist:int,tin:int,tout:int,cancelled:int,cancelcode:chararray,diverted:int,cardelay:int,weatherdelay:int,nasdelay:int,secdelay:int,lateairdelay:int);

B = group A by uniquecarrier;

C = foreach B 
{
	D = COUNT(A);
	E = filter A by arrdelay < 15 and depdelay < 15;
	F = COUNT(E);
	generate group, F,D,(float)F/D;
};

STORE C into 's3://flightdataanalysis/output2b';