# /usr/bin/python
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS

import os
import urllib
import zipfile
import sys
import math

sys.path.append("/Users/edv/spark-2.3.0-bin-hadoop2.7/python/lib/")

sc = SparkContext().getOrCreate()
# create Tranning ALS



#dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
#'http://files.grouplens.org/datasets/movielens/ml-latest.zip'

# Get path
datasets_path = os.path.join('/Users/edv/Documents/ML/RecommendVnsDoc')

# Download file zip
#complete_f = urllib.urlretrieve(dataset_url, "ml-latest.zip")
print ("Dowload Completion")

# unZip
with zipfile.ZipFile("dataset-recommend.zip", "r") as zip_ref:
    zip_ref.extractall("datasets")


seed = 5
iterations = 10
regularization_parameter = 0.1
ranks = [4, 8, 12]
errors = [0, 0, 0]
err = 0
tolerance = 0.02

min_error = float('inf')
best_rank = 12
best_iteration = -1


# Load the complete dataset file
complete_ratings_file = os.path.join(datasets_path, 'datasets', 'ratings.csv')
complete_ratings_raw_data = sc.textFile(complete_ratings_file)
complete_ratings_raw_data_header = complete_ratings_raw_data.take(1)[0]

# Parse
complete_ratings_data = complete_ratings_raw_data.filter(lambda line: line != complete_ratings_raw_data_header) \
    .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()

training_RDD, test_RDD = complete_ratings_data.randomSplit([7, 3], seed=None)

complete_model = ALS.train(ratings= training_RDD, rank= best_rank, iterations= iterations,
                           lambda_=regularization_parameter, seed=seed)

test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

predictions = complete_model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())

print 'For testing data the RMSE is %s' % (error)

# post
complete_post_file = os.path.join(datasets_path, 'datasets', 'post.csv')
complete_post_raw_data = sc.textFile(complete_post_file)
complete_post_raw_data_header = complete_post_raw_data.take(1)[0]

# Parse
complete_post_data = complete_post_raw_data.filter(lambda line: line != complete_post_raw_data_header) \
    .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), tokens[1], tokens[2])).cache()

complete_post_titles = complete_post_data.map(lambda x: (int(x[0]), x[1]))

print "There are %s post in the complete dataset" % (complete_post_titles.count())

def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)

post_ID_with_ratings_RDD = (complete_ratings_data.map(lambda x: (x[1], x[2])).groupByKey())
post_ID_with_avg_ratings_RDD = post_ID_with_ratings_RDD.map(get_counts_and_averages)
post_rating_counts_RDD = post_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

### new rattings
new_user_ID = 0

# The format of each line is (userID, movieID, rating)
new_user_ratings = [
     (0,260,9), # Star Wars (1977)
     (0,1,8), # Toy Story (1995)
     (0,16,7), # Casino (1995)
     (0,25,8), # Leaving Las Vegas (1995)
     (0,32,9), # Twelve Monkeys (a.k.a. 12 Monkeys) (1995)
     (0,335,4), # Flintstones, The (1994)
     (0,379,3), # Timecop (1994)
     (0,296,7), # Pulp Fiction (1994)
     (0,858,10) , # Godfather, The (1972)
     (0,50,8) # Usual Suspects, The (1995)
    ]
new_user_ratings_RDD = sc.parallelize(new_user_ratings)
print 'New user ratings: %s' % new_user_ratings_RDD.take(10)


complete_data_with_new_ratings_RDD = complete_ratings_data.union(new_user_ratings_RDD)


from time import time

t0 = time()
new_ratings_model = ALS.train(complete_data_with_new_ratings_RDD, best_rank, seed=seed,
                              iterations=iterations, lambda_=regularization_parameter)
tt = time() - t0

print "New model trained in %s seconds" % round(tt,3)


new_user_ratings_ids = map(lambda x: x[1], new_user_ratings) # get just movie IDs
# keep just those not on the ID list (thanks Lei Li for spotting the error!)
new_user_unrated_post_RDD = (complete_post_data.filter(lambda x: x[0] not in new_user_ratings_ids).map(lambda x: (new_user_ID, x[0])))

# Use the input RDD, new_user_unrated_movies_RDD, with new_ratings_model.predictAll() to predict new ratings for the movies
new_user_recommendations_RDD = new_ratings_model.predictAll(new_user_unrated_post_RDD)

# Transform new_user_recommendations_RDD into pairs of the form (Movie ID, Predicted Rating)
new_user_recommendations_rating_RDD = new_user_recommendations_RDD.map(lambda x: (x.product, x.rating))

new_user_recommendations_rating_title_and_count_RDD = \
    new_user_recommendations_rating_RDD.join(complete_post_titles).join(post_rating_counts_RDD)

new_user_recommendations_rating_title_and_count_RDD.take(3)

new_user_recommendations_rating_title_and_count_RDD = \
    new_user_recommendations_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))


top_movies = new_user_recommendations_rating_title_and_count_RDD.filter(lambda r: r[2]>=25).takeOrdered(25, key=lambda x: -x[1])

print ('TOP recommended movies (with more than 25 reviews):\n%s' %
        '\n'.join(map(str, top_movies)))

my_movie = sc.parallelize([(0, 500)]) # Quiz Show (1994)
individual_movie_rating_RDD = new_ratings_model.predictAll(new_user_unrated_post_RDD)
individual_movie_rating_RDD.take(1)


# from pyspark.mllib.recommendation import MatrixFactorizationModel
#
# model_path = os.path.join(datasets_path, 'models', 'movie_lens_als')
#
# # Save and load model
# new_ratings_model.save(sc, model_path)
# same_model = MatrixFactorizationModel.load(sc, model_path)