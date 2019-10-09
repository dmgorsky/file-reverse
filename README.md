#### TASK
Write a program in a language of your choosing that takes a file and reverses it (e.g. “abc de” -> “ed cba”) and provides the 5 most frequent words. The only restriction is that you can’t assume the file will fit in memory. You can make other assumptions that would simplify the code needed but please write them down explicitly.

#### SOLUTION
I read rows from file (splitting it if big size) and ingested them into an embedded database to create a dataframe for Spark to work with.
Then just used spark sql to process it.

