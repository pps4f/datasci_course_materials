-- register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar
register ./myudfs.jar

-- load the test file into Pig
-- raw = LOAD '/home/ubuntu/btc100' USING TextLoader as (line:chararray);
raw = LOAD '/home/ubuntu/cse344-test-file' USING TextLoader as (line:chararray);
-- raw = LOAD '/home/ubuntu/btc-2010-chunk-000' USING TextLoader as (line:chararray);
-- raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file' USING TextLoader as (line:chararray);
-- later you will load to other files, example:
-- raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray); 

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject1:chararray,predicate1:chararray,object1:chararray);

-- filter for subject matchesxs
filtered1 = filter ntriples by subject1 matches '.*business.*';

filtered2 = foreach filtered1 generate * as (subject2:chararray,predicate2:chararray,object2:chararray);

joined = join filtered1 by object1, filtered2 by subject2;

dump joined;

--group the n-triples by subject column
subjects = group ntriples by (subject) PARALLEL 50;

-- flatten the objects out (because group by produces a tuple of each object
-- in the first column, and we want each subject to be a string, not a tuple),
-- and count the number of tuples associated with each subject
count_by_subject = foreach subjects generate flatten($0), COUNT(ntriples) as count PARALLEL 50;

-- group the subject counts
group_by_subject_count = group count_by_subject by count;

-- count up the subject counts
count_by_subj_count = foreach group_by_subject_count generate group, COUNT(count_by_subject) as count PARALLEL 50;

-- order the subject counts
count_by_subj_count_ordered = order count_by_subj_count by (group) PARALLEL 50;

-- dump count_by_subj_count_ordered;
store count_by_subj_count_ordered into '/user/hadoop/example-results' using PigStorage();





