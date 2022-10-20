#!/usr/bin/env python
# coding: utf-8



import os
import pandas as pd
import json
import textblob
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# gender inference methodology ---------------------------------------

# tidying names for step 2 in methodology

df_names_male = pd.read_csv('./male_names.tsv.gz', sep='\t')
#df_names_male = pd.read_csv('./names/male_names.tsv', sep='\t')

df_names_male = df_names_male[df_names_male.year > 1920]

df_names_male = df_names_male.\
    groupby("name", as_index = False)[["count"]].\
    sum().\
    sort_values(by = 'count', ascending = False)
df_names_male = df_names_male[df_names_male["count"] > 500]

# filtering by > 500 makes the count go from 38022 to 6235 


df_names_female = pd.read_csv('./female_names.tsv.gz', sep='\t')

#df_names_female = pd.read_csv('./names/female_names.tsv', sep='\t')
df_names_female = df_names_female[df_names_female.year > 1920]

df_names_female = df_names_female.\
    groupby("name", as_index = False)[["count"]].\
    sum().\
    sort_values(by = 'count', ascending = False)
df_names_female = df_names_female[df_names_female["count"] > 500]

# filtering by > 500 makes the count go from 63287 to 9547 

df_names_male["name"] = df_names_male["name"].map(lambda x: x.lower())
df_names_female["name"] = df_names_female["name"].map(lambda x: x.lower())

# common names get assigned to more frequent gender
inner_join = pd.merge(df_names_male, df_names_female, on = "name")
inner_join["remove_male"] = inner_join["count_x"] < inner_join["count_y"]
inner_join
# 1384 names in common
# 634 will be taken out from males and  750 from the females


# sets have fast search
names_male = set(df_names_male["name"])
names_female = set(df_names_female["name"])

remove_male = set(inner_join[inner_join.remove_male == True].name)
remove_female = set(inner_join[inner_join.remove_male == False].name)

names_male = names_male.difference(remove_male)
names_female = names_female.difference(remove_female)

# words used in step 3
words_male = {"male", "father", "son", "brother", "man", "boy", "hombre", "padre", "hijo"}
words_female = {"female", "mother", "daughter", "sister", "woman", "girl", "mujer", "madre", "hija"}
words_nb = {"nb", "nonbinary", "binarie"}

# gender inference function
def check_gender (token_bio, token_name):
    
    # 1. self identification
    she = {"she/her"}
    he = {"he/him"}
    they = {"they/them"}
    
    if  (len(token_bio.intersection(she)) > 0):
        return "female"
    elif (len(token_bio.intersection(he)) > 0):
        return "male"
    elif  (len(token_bio.intersection(they)) > 0):
        return "nonbinary"
    
        # 2. Check name
    elif  (len(token_name.intersection(names_female)) > 0):
        return "female"
    elif (len(token_name.intersection(names_male)) > 0):
        return "male"
    
    # 3. Check bio
    elif  (len(token_bio.intersection(words_female)) > 0):
        return "female"
    elif (len(token_bio.intersection(words_male)) > 0):
        return "male"
    elif  (len(token_bio.intersection(words_nb)) > 0):
        return "nonbinary"

    else:
        return "???"
        






# main function ----------------------------------------------------------

def analysis(filtered):

    column_names = ["time", "rounded_datetime", "tweet_type", "og_author_name", "og_author_desc", "tweet_author_name", "tweet_author_desc"]

    if (filtered == True):
        #data_tw = pd.read_csv('output_ta_change_next_line.csv', na_filter = False, on_bad_lines='skip', lineterminator='\n', names = column_names)
        data_tw = pd.read_csv('output_keyword.csv', na_filter = False, on_bad_lines='skip', lineterminator='\n', names = column_names)

    else:
        #data_tw = pd.read_csv('output_random_24hrs.csv', na_filter = False, on_bad_lines='skip', lineterminator='\n', names = column_names)
        data_tw = pd.read_csv('output_random.csv', na_filter = False, on_bad_lines='skip', lineterminator='\n', names = column_names)

    data_tw = data_tw[data_tw.tweet_type != ""]

    # tokenizing
    data_tw["token_bio_og"] = data_tw["og_author_desc"].map(lambda x: textblob.TextBlob(x.lower()).words)
    data_tw["token_bio_og"] = data_tw["token_bio_og"].map(set)

    data_tw["token_name_og"] = data_tw["og_author_name"].map(lambda x: textblob.TextBlob(x.lower()).words)
    data_tw["token_name_og"] = data_tw["token_name_og"].map(set)


    data_tw["token_bio_tweet"] = data_tw["tweet_author_desc"].map(lambda x: textblob.TextBlob(x.lower()).words)
    data_tw["token_bio_tweet"] = data_tw["token_bio_tweet"].map(set)

    data_tw["token_name_tweet"] = data_tw["tweet_author_name"].map(lambda x: textblob.TextBlob(x.lower()).words)
    data_tw["token_name_tweet"] = data_tw["token_name_tweet"].map(set)



    # gender inference
    data_tw["gender_og"] = np.vectorize(check_gender)(data_tw["token_bio_og"], data_tw["token_name_og"])
    data_tw["gender_tweet"] = np.vectorize(check_gender)(data_tw["token_bio_tweet"], data_tw["token_name_tweet"])
    data_tw[(data_tw.gender_og != "???") | (data_tw.gender_tweet != "???" )]
    # twitter data is 60% male and 40% female in the US (but mostly english so ok) https://www.oberlo.com/blog/twitter-statistics#:~:text=3.-,Twitter%20Demographics%3A%20Gender,females%20at%202.5%20to%20one.


    data_both = data_tw[(data_tw.gender_og != "???") & (data_tw.gender_tweet != "???" )]
    data_both = data_both[(data_both.gender_tweet != "nonbinary") & (data_both.gender_og != "nonbinary")]

    #percent_female_og = len(data_tw[(data_tw.gender_og == "female")])/len(data_tw[(data_tw.gender_og == "female") | (data_tw.gender_og == "male")])
    #percent_female_tweet = len(data_tw[(data_tw.gender_tweet == "female")])/len(data_tw[(data_tw.gender_tweet == "female") | (data_tw.gender_tweet == "male")])
    #percent_female_og
    #percent_female_tweet

    data_both["RT:"] = data_both.gender_tweet + " to " + data_both.gender_og 


    # plot ----------------------------------------------------------------------------------
    data_plot = data_both.\
        groupby(["RT:", "rounded_datetime"], as_index = False).\
        agg(Frequency = ("RT:", "count"))

    data_plot['Time'] = data_plot['rounded_datetime'].apply(lambda row: row[11:16])


    sns.set_style("whitegrid")

    #plot = sns.scatterplot(
    plot = sns.lineplot(
        data = data_plot,
        x = "Time", y = "Frequency", 
        hue="RT:"
    )

    plot.set_ylim(0, data_plot.max()["Frequency"]+ 50)

    for ind, label in enumerate(plot.get_xticklabels()):
        if ind % 3 == 0:  # every 3rd label is kept
            label.set_visible(True)
        else:
            label.set_visible(False)


    if (filtered == True):
        plot.set(title='Retweet Frequency in Filtered Sample')
        plot.figure.savefig('homophily_work.png', dpi=300)

    else:
        plot.set(title='Retweet Frequency in Random Sample')
        plot.figure.savefig('homophily_random.png', dpi=300)

    plt.clf()


    # conditional probability --------------------------------------------------

    n = len(data_both)
    p_male = sum(data_both.gender_tweet == "male")/n
    p_female = sum(data_both.gender_tweet == "female")/n

    p_female_to_female = sum(data_both["RT:"] == "female to female")/n
    p_male_to_male = sum(data_both["RT:"] == "male to male")/n
    p_female_to_male = sum(data_both["RT:"] == "female to male")/n
    p_male_to_female = sum(data_both["RT:"] == "male to female")/n


    p_rt_female_female = p_female_to_female/p_female
    p_rt_male_male = p_male_to_male/p_male
    p_rt_male_female = p_female_to_male/p_female
    p_rt_female_male = p_male_to_female/p_male

    print("p(male) = ",np.round(p_male, decimals = 3))
    print("p(female) = ",np.round(p_female, decimals = 3))

    print("p(RT female | female) =", np.round(p_rt_female_female, decimals = 3))
    print("p(RT male | male) =", np.round(p_rt_male_male, decimals = 3))

    print("p(RT male | female) =", np.round(p_rt_male_female, decimals = 3))
    print("p(RT female | male) =", np.round(p_rt_female_male, decimals = 3))

    # group by time and we plot it!


    data_probs = data_both.\
        groupby(["rounded_datetime"], as_index = False).\
        agg(n = ("RT:", "count") ,
        p_male = ("gender_tweet", lambda x: sum(x == "male")),
        p_female = ("gender_tweet", lambda x: sum(x == "female")),
        p_female_to_female = ("RT:", lambda x: sum(x == "female to female")),
        p_male_to_male = ("RT:", lambda x: sum(x == "male to male")),
        p_male_to_female = ("RT:", lambda x: sum(x == "male to female")),
        p_female_to_male = ("RT:", lambda x: sum(x == "female to male"))
        ).\
        assign(
            p_male = lambda x: x.p_male/x.n,
            p_female = lambda x: x.p_female/x.n,
            p_female_to_female = lambda x: x.p_female_to_female/x.n,
            p_male_to_male = lambda x: x.p_male_to_male/x.n,
            p_male_to_female = lambda x: x.p_male_to_female/x.n,
            p_female_to_male = lambda x: x.p_female_to_male/x.n
        ).\
        assign(
            p_rt_female_female = lambda x: x.p_female_to_female/x.p_female,
            p_rt_male_male = lambda x: x.p_male_to_male/x.p_male,
            p_rt_male_female = lambda x: x.p_female_to_male/x.p_female,
            p_rt_female_male = lambda x: x.p_male_to_female/x.p_male
        )




    data_bayes =  pd.melt(
        data_probs[["rounded_datetime", "p_rt_female_female", "p_rt_male_male", "p_rt_male_female", "p_rt_female_male"]], 
        id_vars = ["rounded_datetime"], 
        var_name = "p_type", 
        value_name = "Probability"
    )

    def change_name_prob (name):
        if name == "p_rt_female_female":
            return("P(RT Female | Female)")
        elif name == "p_rt_male_male":
            return("P(RT Male | Male)")
        elif name == "p_rt_female_male":
            return("P(RT Female | Male)")
        elif name == "p_rt_male_female":
            return("P(RT Male | Female)")
        else:
            return ""


    data_bayes["Conditional Probability"] = data_bayes.p_type.map(change_name_prob)
    data_bayes['Time'] = data_bayes['rounded_datetime'].apply(lambda row: row[11:16])


    # plot conditional probability --------------------------------------------
    sns.set_style("whitegrid")

    # Create a visualization
    #plot = sns.scatterplot(
    plot2 = sns.lineplot(
        data = data_bayes,
        x = "Time", y = "Probability", 
        hue="Conditional Probability"
    )

    plot2.set_ylim(0, 1) 

    

    for ind, label in enumerate(plot2.get_xticklabels()):
        if ind % 3 == 0:  # every 3rd label is kept
            label.set_visible(True)
        else:
            label.set_visible(False)


    if (filtered == True):
        plot2.set(title='Conditional Probability in Filtered Sample')
        plot2.figure.savefig('bayes_work.png', dpi=300)

    else:
        plot2.set(title='Conditional Probability in Random Sample')
        plot2.figure.savefig('bayes_random.png', dpi=300)
    
    plt.clf()



    
# we run the analysis first for the filtered and then for the random sample
analysis(filtered = True)
analysis(filtered = False)