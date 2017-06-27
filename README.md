# twitter_influencers
Code to create a real time streaming platform that helps marketers identify top influencers on Twitter. 

We created a real time streaming platform using Twitter streaming API and Google NLP (Natural Language Processing) API. The streaming data was sent to Amazon S3 and batch processed using Apache Spark. The final output from our analysis in Spark was sent to Amazon RDS as a SQL table and updated every 15 minutes. The Python Flask App called the database to display the real time data to the client interface.

Link to blog post: http://blog.nycdatascience.com/student-works/creating-real-time-streaming-platform-identify-top-influencers-twitter/
