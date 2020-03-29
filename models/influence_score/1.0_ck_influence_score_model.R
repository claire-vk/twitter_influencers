##### USING THE DATASET FROM KAGGLE COMPETITION: https://www.kaggle.com/c/predict-who-is-more-influential-in-a-social-network #####

#import dataset
setwd('xxx')

traindata = read.csv("train.csv")
testdata = read.csv("test.csv")

#normalize data except for choice column
traindataNorm = scale(traindata[2:23])
traindataNorm = cbind(traindata[1], traindataNorm)
testdataNorm = scale(testdata[1:22])

library(caret)

##### LOGISTIC REGRESSION #####
# Train LR model
modelLR = train(as.factor(Choice) ~., 
                family = binomial(link='logit'),
                data = traindataNorm, 
                method="glm")

# Look at variable importance
varImp(modelLR)

# Overall
# B_listed_count      100.000
# A_listed_count       93.212
# A_network_feature_1  67.192
# A_retweets_received  41.445
# B_retweets_sent      30.455
# B_network_feature_3  26.511
# A_network_feature_3  26.210
# A_follower_count     22.360
# B_follower_count     21.496
# B_network_feature_1  18.768
# A_retweets_sent      18.724
# A_network_feature_2  18.188
# B_mentions_sent      15.766
# B_network_feature_2  13.547
# A_following_count    12.470
# B_retweets_received  11.305
# A_mentions_sent      11.157
# A_posts               7.538
# A_mentions_received   7.496
# B_following_count     2.431


##### Feature engineering #####
# use the same features as we'll use in influence table
# ratio followers / following:
traindataNorm$ratio_followers_following_A = traindataNorm$A_follower_count / traindataNorm$A_following_count
traindataNorm$ratio_followers_following_B = traindataNorm$B_follower_count / traindataNorm$B_following_count

# social reputation:
traindataNorm$reputation_A = traindataNorm$A_follower_count * traindataNorm$A_posts / traindataNorm$A_following_count
traindataNorm$reputation_B = traindataNorm$B_follower_count * traindataNorm$B_posts / traindataNorm$B_following_count

# retweet_retweeted_ratio: 
traindataNorm$retweet_retweeted_ratio_A = traindataNorm$A_retweets_received / traindataNorm$A_retweets_sent
traindataNorm$retweet_retweeted_ratio_B = traindataNorm$B_retweets_received / traindataNorm$B_retweets_sent

# retweet_ratio
traindataNorm$retweet_ratio_A = traindataNorm$A_retweets_received / traindataNorm$A_posts
traindataNorm$retweet_ratio_B = traindataNorm$B_retweets_received / traindataNorm$B_posts

# mentions_ratio:
traindataNorm$mentions_ratio_A = traindataNorm$A_mentions_sent / traindataNorm$A_posts
traindataNorm$mentions_ratio_B = traindataNorm$B_mentions_sent  / traindataNorm$B_posts

# retweets_given_ratio:
traindataNorm$retweets_given_ratio_A = traindataNorm$A_retweets_sent / traindataNorm$A_posts
traindataNorm$retweets_given_ratio_B = traindataNorm$B_retweets_sent / traindataNorm$B_posts


# retrain with feature engineering
modelLR_eng = train(as.factor(Choice) ~., 
                family = binomial(link='logit'),
                data = traindataNorm, 
                method="glm")

# Look at variable importance with feature engineering
varImp(modelLR_eng)

# B_listed_count              100.000
# A_listed_count               95.115
# B_retweets_sent              30.645
# retweet_ratio_A              26.844
# B_network_feature_3          25.633
# A_retweets_sent              24.767
# A_network_feature_1          24.600
# A_network_feature_3          24.474
# A_follower_count             23.277
# B_follower_count             21.632
# reputation_B                 21.308
# reputation_A                 19.705
# A_network_feature_2          17.084
# B_network_feature_2          12.957
# A_posts                      12.946
# A_mentions_received          11.792
# A_following_count            11.210
# ratio_followers_following_B  10.560
# B_network_feature_1          10.302
# B_mentions_sent               9.963



##### RANDOM FOREST #####
# do variable importance using rf
library(randomForest)
library(dplyr)

set.seed(0)
rf.default = randomForest(as.factor(Choice) ~., data = traindataNorm, importance = TRUE)
importance_df = importance(rf.default)
importance_df = as.data.frame(importance_df)
importance_df[with(importance_df, order(MeanDecreaseGini, decreasing = T)), ] %>% 
  select(MeanDecreaseGini)


varImpPlot(rf.default)


#                                   MeanDecreaseGini
# A_follower_count                   170.00737
# B_network_feature_1                164.34350
# A_listed_count                     160.53411
# B_listed_count                     154.64400
# B_mentions_received                148.03239
# A_network_feature_1                142.34424
# A_mentions_received                134.01579
# B_follower_count                   125.09368
# B_retweets_received                113.49543
# A_retweets_received                100.59072
# ratio_followers_following_A         75.68351
# ratio_followers_following_B         65.97614
# A_network_feature_3                 63.86652
# reputation_A                        62.30107
# reputation_B                        61.65115
# B_network_feature_3                 60.05937
# A_following_count                   59.51146
# B_network_feature_2                 58.72433
# B_posts                             58.22978
# A_network_feature_2                 57.32207
# B_following_count                   56.46618
# retweet_retweeted_ratio_A           56.09488
# retweet_retweeted_ratio_B           55.41335
# B_mentions_sent                     53.04946
# A_posts                             52.72989
# A_mentions_sent                     52.02837
# retweet_ratio_B                     49.69503
# retweet_ratio_A                     49.58363
# mentions_ratio_A                    49.42406
# retweets_given_ratio_B              48.22145
# retweets_given_ratio_A              47.06635
# mentions_ratio_B                    46.91836
# B_retweets_sent                     34.27663
# A_retweets_sent                     31.09103






##### PCA #####
library(psych)
fa.parallel(traindataNorm[, -1], fa = "pc", n.iter = 100)  # 8 PCs seem to be the right number

pc_train = principal(traindataNorm[, -1], nfactors = 8, rotate = "none")
pc_train

factor.plot(pc_train, labels = colnames(traindataNorm))
