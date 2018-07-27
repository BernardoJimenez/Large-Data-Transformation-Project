# Large-Data-Transformation-Project
A project utilizing MySQL, Python, REST API terminology, and unit testing on data too large to fit in memory...

FAQ:
> What am I looking at exactly?
This is an assignment provided to me by a company as an assessment. 

> Who gave you this assignment?
I can't say. Not that there was a non-disclosure or anything like that, but out of courtesy.

> Isn't it in bad taste to post your solution at all?
I removed all information that could be traced back to the initial company giving the assignment. Someone googling
for answers to this assignment is not going to find mine. Additionally, I worked on this assignment unpaid for
5 hours before I even got a phone screening. I wouldn't let such good work go to waste.

> If your solution is so great, why didn't you get the job?
I was told I didn't have enough experience in Spark and Scala for this (entry-level) position.

> So, does the code run?
It did, but I changed all variables and components to make it unidentifiable with the company's assignment, so it
won't work if you try to run it on your machine. However, if you read the Assignment Instructions PDF, you'll see
that my code follows all conventions necessary to theoretically run. If I changed it back to the original it would
run fune and produce the output described in the PDF.

> Where can I find the original assignment instructions?
I wrote them up in the Assignment Instructions PDF included here.

> Why did you use mysqlclient over other SQL modules?
mysqlclient for Python 3 is most compatible with MySQLDB since it is a fork, and
has the fastest implementation since it is C based rather than pure Python.

> Why did you comment your code so heavily?
You never know the technical prowess of someone who might read it later, so
understanding it from comments makes it easier for me and others in the future
to understand. Additionally, I wanted to offer insight on choices I made while
coding during the assignment.

> Did you try to make your runtime as small as possible?
I tried to save time and space where I could, but was bummed there was no
obvious way to parallelize my code with multiprocessing. The reason is since
I am writing to a .CSV file, rows could get completely jumbled and written
in the middle of another. One way to solve for this is to have a dedicated
producer thread and a writer thread that had a queue of data from the producer,
but since the producer would be producing dataframes of several thousand 
rows 64 times, I would run into memory issues with the queue since all the data
can not be stored in memory at once.

> Why did you write JSON values to the output .CSV file with square brackets?
The assignment instructions specifiy a '... JSON list of "job" and "company" 
key/value pairs'. Since there is no structure in JSON formally known as just
a 'list', I decided to put the key-value pairs in a JSON array, which acts as
an indexable list. If instead you want a simple key-value pair dictionary,
then simply remove the brackets from the JSON value creation in lines 79 & 89-ish
from the assignment Python file.

> Why did your SQL CREATE TABLE statement use JSON data types instead of string?
JSON data types are included in MySQL, and offers better integration of data
with custom attributes. Using a varchar would work as well, but you would not
be able to read the JSON string from MySQL itself if stored like this. With
the data type as JSON, other parts of data stored in that JSON list should still
be accessible through MySQL and through other programs interacting with it such
as Python.

> Why did your SQL CREATE TABLE statement add an 'id' field as a primary key?
I though that the emd5 string would act as a complicated and easy to misspell
primary key, so I added an 'id' field as a primary key with the same properties
as the 'id' field in the BusinessA and FirmB data sets. Note: the ids between
data sets do NOT match. They work only as a primary key for their respective
tables.

> Why do you use the 'with' keyword when opening files in some areas, and in
other areas you manually open and close the file?
The 'with' keyword is useful for avoiding file errors from the file not 
existing. However, if I'm using the file repeatedly, it's more expensive to
continuously open and close the file. So I left it open for the big for-loop
and closed it after. I also only opened it manually when I was sure it had
already been created in the preceding code.

> What prior experience do you have with Python and SQL?
I'm mostly self taught in Python, and supplemented that by earning a
Specialization in Applied Data Science with Python from the University of
Michigan through Coursera. As for SQL, I worked with PL/SQL and Excel
extensively while interning for tech research firm Gartner.


> Could you think of any other suitable solutions?
I think using an Amazon Web Service such as Amazon Athena could provide a
quicker solution. As I understand it, you would be able to load both data sets
onto the same server and perform queries on the tables and their union. The 
reason I did not attempt this is because it would show less of my Python and SQL
knowledge than my AWS knowledge. Perhaps Athena may not the the correct service,
but I know for sure it could be done through AWS.

> You looking for a job still?
Yes. Find me on LinkedIn. Looking for work as a Junior Data Engineer or Software Engineer in
either of the greater NYC or Boston areas.
