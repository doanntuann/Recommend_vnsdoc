import os
from pyspark.mllib.recommendation import ALS

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
    """Given a tuple (postID, ratings_iterable) 
    returns (postID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1])) / nratings)


class RecommendationEngine:
    """A post recommendation engine
    """

    def __count_and_average_ratings(self):
        """Updates the posts ratings counts from 
        the current data self.ratings_RDD
        """
        logger.info("Counting post ratings...")
        post_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        post_ID_with_avg_ratings_RDD = post_ID_with_ratings_RDD.map(get_counts_and_averages)
        self.posts_rating_counts_RDD = post_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
                               iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("ALS model built!")

    def __predict_ratings(self, user_and_post_RDD):
        """Gets predictions for a given (userID, postID) formatted RDD
        Returns: an RDD with format (postTitle, postRating, numRatings)
        """
        predicted_RDD = self.model.predictAll(user_and_post_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.posts_titles_RDD).join(self.posts_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

        return predicted_rating_title_and_count_RDD

    def add_ratings(self, ratings):
        """Add additional post ratings in the format (user_id, post_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings_RDD = self.sc.parallelize(ratings)
        # Add new ratings to the existing ones
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        # Re-compute post ratings count
        self.__count_and_average_ratings()
        # Re-train the ALS model with the new ratings
        self.__train_model()

        return ratings

    def get_ratings_for_post_ids(self, user_id, post_ids):
        """Given a user_id and a list of post_ids, predict ratings for them 
        """
        requested_posts_RDD = self.sc.parallelize(post_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        ratings = self.__predict_ratings(requested_posts_RDD).collect()

        return ratings

    def get_top_ratings(self, user_id, posts_count):
        """Recommends up to posts_count top unrated posts to user_id
        """
        # Get pairs of (userID, postID) for user_id unrated posts
        user_unrated_posts_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id) \
            .map(lambda x: (user_id, x[1])).distinct()
        # Get predicted ratings
        ratings = self.__predict_ratings(user_unrated_posts_RDD).filter(lambda r: r[2] >= 25).takeOrdered(posts_count,
                                                                                                           key=lambda
                                                                                                               x: -x[1])

        return ratings

    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'datasets', 'ratings.csv')
        #os.path.join(dataset_path, 'ratings.csv')
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line != ratings_raw_data_header) \
            .map(lambda line: line.split(",")).map(
            lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()
        # Load posts data for later use
        logger.info("Loading posts data...")
        posts_file_path = os.path.join(dataset_path, 'datasets', 'posts.csv')
        posts_raw_RDD = self.sc.textFile(posts_file_path)
        posts_raw_data_header = posts_raw_RDD.take(1)[0]
        self.posts_RDD = posts_raw_RDD.filter(lambda line: line != posts_raw_data_header) \
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), tokens[1], tokens[2])).cache()
        self.posts_titles_RDD = self.posts_RDD.map(lambda x: (int(x[0]), x[1])).cache()
        # Pre-calculate posts ratings counts
        self.__count_and_average_ratings()

        # Train the model
        self.rank = 8
        self.seed = 5L
        self.iterations = 10
        self.regularization_parameter = 0.1
        self.__train_model() 
