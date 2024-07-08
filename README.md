My semester project for the Big Data Analytics course involved creating a live product recommender system using the extensive Amazon product dataset, which was a hefty 130GBs when extracted. The project unfolded in two phases, addressing crucial aspects of data processing, analysis, model training, and live streaming.

In the first phase, I managed loading the Amazon product dataset into MongoDB with Apache Spark for efficiency and scalability. I carefully considered the dataset's schema and optimized data loading while handling any anomalies to ensure integrity and reliability.

After loading the data, I conducted exploratory data analysis (EDA) using Pandas and Matplotlib to extract insights and identify patterns. Through statistical analysis, I uncovered valuable insights guiding subsequent stages.

I then moved to data preprocessing and cleaning, addressing missing values and transforming variables to prepare the dataset for model training.

In Phase 2, I focused on model training and live streaming, utilizing machine learning algorithms like Alternating Least Squares (ALS) for recommendation. I explored collaborative filtering, content-based filtering, and matrix factorization techniques, evaluating performance using precision, recall, and F1 score metrics.

Simultaneously, I built a Flask-based web application for accessing recommendations, storing user preferences and recommendations in MongoDB. REST APIs facilitated communication between the web app and recommendation system.

Lastly, I integrated Apache Kafka for real-time streaming of recommendations based on user preferences, enhancing system responsiveness and adaptability.

This project provided hands-on experience in big data processing, NoSQL databases, machine learning, web development, and stream processing, honing problem-solving, critical thinking, and communication skills for success in data analytics and technology.
