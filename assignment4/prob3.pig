register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar
-- register ./myudfs.jar

set default_parallel 50;

-- load the test file into Pig
-- raw = LOAD '/home/ubuntu/btc100' USING TextLoader as (line:chararray);
-- raw = LOAD '/home/ubuntu/cse344-test-file' USING TextLoader as (line:chararray);
-- raw = LOAD '/home/ubuntu/btc-2010-chunk-000' USING TextLoader as (line:chararray);
-- raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file' USING TextLoader as (line:chararray);
-- later you will load to other files, example:
-- raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray); 
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-*' USING TextLoader as (line:chararray);

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject1:chararray,predicate1:chararray,object1:chararray);

-- filter for subject matches
filtered1 = filter ntriples by subject1 matches '.*rdfabout\\.com.*';

filtered2 = foreach filtered1 generate * as (subject2:chararray,predicate2:chararray,object2:chararray);

joined = join filtered1 by object1, filtered2 by subject2 parallel 50;

distinct_list = distinct joined parallel 50;

-- Save file
-- store joined into '/user/hadoop/example-results' using PigStorage();

store distinct_list into '/user/hadoop/example-results' using PigStorage();





