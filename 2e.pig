A = load 's3://flightdataanalysis/CSVFiles/*.csv' using PigStorage(',') as
(year:int,month:int,date:int,day:int,deptime:int,crsdeptime:int,arrtime:int,crsarrtime:int,uniquecarrier:chararray,fnum:int,tnum:int,etime:int,crsetime:int,atime:int,arrdelay:int,depdelay:int,org:chararray,
dest:chararray,dist:int,tin:int,tout:int,cancelled:int,cancelcode:chararray,diverted:int,cardelay:int,weatherdelay:int,nasdelay:int,secdelay:int,lateairdelay:int);

B = load 's3://assignment1/airports.csv' using PigStorage(',') as
(iata:chararray,airport:chararray,city:chararray,state:chararray,country:chararray,lat:long,longt:long);

C = group A by dest;
D = foreach C generate group as ndata, COUNT(A) as fraction;
E = order D by fraction desc;

F = limit E 3;

G = foreach F generate ndata as nd;
H = foreach B generate REPLACE($0,'"','') as iata, airport;
I = foreach H generate iata, airport;
J = join G by nd, I by iata;

store J into 's3://flightdataanalysis/output2e';