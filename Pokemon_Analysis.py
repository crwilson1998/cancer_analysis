# Import modules
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
import numpy as np

# Optional - Choose style of graph background and how many columns show
plt.style.use('ggplot')
pd.set_option('max_columns', 25)

# Import the dataset and explore. I want to see if there are any intial changes I need to make before analyzing the stats
pokemon = pd.read_csv(r"C:\Users\Chris\PycharmProjects\Projects\DA_Projects\pokemon\Pokemon.csv", index_col=False)
print(pokemon.head(10))
print(pokemon.tail(10))
print(pokemon.columns)
print(pokemon.shape)
print(pokemon.dtypes)
print(pokemon.describe())
print(pokemon.isna().sum())
print(pokemon.loc[pokemon.duplicated()])

# Cleaning the data for later use

# I want to change the (#) column to Number for easier readability, remove legendaries since I want a team for normal non legendary pokemon and
# remove mega evolutions since they have not been introduced into the game
non_legendary = pokemon[pokemon['Legendary'] == False]
non_legendary.rename({'#': 'Number'},axis=1,inplace=True)
non_legendary = non_legendary.set_index('Number')
non_legendary.drop('Legendary', inplace=True, axis=1)
non_legendary = non_legendary[non_legendary['Name'].str.contains("Mega") == False]
# print(non_legendary.columns)
# print(non_legendary.head(10))


# Seeing how many non_legendary pokemon are in each region. My goal here is to see the distribution of non_legendary pokemon bewtween the different
# generation. I hope that there is not a large difference in the total amount of non_legendary
# ax = non_legendary.groupby('Generation') \
#             .size() \
#             .plot(kind='bar',
#                   title='Number of Pokemon Per Generation')
# ax.set_xlabel('Generation')
# ax.set_ylabel('Number of Pokemon')
# plt.show()


# Viewing the distribution of the Attack, Defense, and Speed stats. I want to check for outliers as this might also skew
# the data if there are too many
# sns.boxplot(data=non_legendary,x='Generation',y='Attack')\
#             .set(title='Attack Distribution by Generation')
# plt.show()
# sns.boxplot(data=non_legendary,x='Generation',y='Defense')\
#             .set(title='Defense Distribution by Generation')
# plt.show()
# sns.boxplot(data=non_legendary,x='Generation',y='Speed')\
#             .set(title='Speed Distribution by Generation')
# plt.show()

# 1 , 4, 5 are the generation with higher average stats and have the pokemon with the highest stats in the fields

# Viewing the correlation between all stats. One thing to note is while my main focus is Attack, Defense, and Speed, the
# "special statistics" are something to also consider when choosing a team
sns.pairplot(non_legendary,vars=['Attack','Defense','Sp. Atk','Sp. Def','Speed'],hue='Generation')
plt.show()

pokemon_corr = non_legendary[['HP','Attack','Defense','Sp. Atk','Sp. Def','Speed']].corr()
sns.heatmap(pokemon_corr,annot=True)
plt.title('Correlation Between Different Pokemon Stats')
plt.show()

# minor correltion between defense and sp. def none between atk and sp. atk speed and defense negative correlation 

# Now that we seen the amount of non_legendary pokemon per generation, the distribution between the main stats, as well as their correlation to the special stats
# I want to see which types on average have the highest amongst the three main stats
# avg_atk = non_legendary['Attack'].mean().round(2)
# pokemon_agg = non_legendary.groupby('Type 1') \
#                      .agg(Attack = ('Attack','mean')) \
#                      .round(2) \
#                      .reset_index()
# pokemon_agg['Hex Color'] = ['#A6B91A','#705746','#6F35FC','#F7D02C','#D685AD','#C22E28','#EE8130','#A98FF3','#735797','#7AC74C','#E2BF65','#96D9D6','#A8A77A','#A33EA1','#F95587','#B6A136','#B7B7CE','#6390F0']
# plt.barh(pokemon_agg['Type 1'],pokemon_agg['Attack'],color=pokemon_agg['Hex Color'])
# plt.axvline(x=avg_atk,color='black',linestyle='dashed',label='Average Attack Stat')
# plt.xlabel('Pokemon Type')
# plt.ylabel('Attack Stat')
# plt.legend(loc='upper right')
# plt.show()
#
# avg_def = non_legendary['Defense'].mean().round(2)
# pokemon_agg = non_legendary.groupby('Type 1') \
#                      .agg(Defense = ('Defense','mean')) \
#                      .round(2) \
#                      .reset_index()
# pokemon_agg['Hex Color'] = ['#A6B91A','#705746','#6F35FC','#F7D02C','#D685AD','#C22E28','#EE8130','#A98FF3','#735797','#7AC74C','#E2BF65','#96D9D6','#A8A77A','#A33EA1','#F95587','#B6A136','#B7B7CE','#6390F0']
# plt.barh(pokemon_agg['Type 1'],pokemon_agg['Defense'],color=pokemon_agg['Hex Color'])
# plt.axvline(x=avg_def,color='black',linestyle='dashed',label='Average Defense Stat')
# plt.xlabel('Pokemon Type')
# plt.ylabel('Defense Stat')
# plt.legend(loc='upper right')
# plt.show()
#
# avg_spd = non_legendary['Speed'].mean().round(2)
# pokemon_agg = non_legendary.groupby('Type 1') \
#                      .agg(Speed = ('Speed','mean')) \
#                      .round(2) \
#                      .reset_index()
# pokemon_agg['Hex Color'] = ['#A6B91A','#705746','#6F35FC','#F7D02C','#D685AD','#C22E28','#EE8130','#A98FF3','#735797','#7AC74C','#E2BF65','#96D9D6','#A8A77A','#A33EA1','#F95587','#B6A136','#B7B7CE','#6390F0']
# plt.barh(pokemon_agg['Type 1'],pokemon_agg['Speed'],color=pokemon_agg['Hex Color'])
# plt.axvline(x=avg_spd,color='black',linestyle='dashed',label='Average Speed Stat')
# plt.xlabel('Pokemon Type')
# plt.ylabel('Speed Stat')
# plt.legend(loc='upper right')
# plt.show()



# Cleaning the data for later use but this time keeping only legendaries in the result set

legendary = pokemon[pokemon['Legendary'] == True]
legendary.rename({'#': 'Number'},axis=1,inplace=True)
legendary = legendary.set_index('Number')
legendary.drop('Legendary', inplace=True, axis=1)
legendary = legendary[legendary['Name'].str.contains("Mega") == False]
# print(legendary.columns)
# print(legendary.head(10))


# Seeing how many legendary pokemon are in each region. My goal here is to see the distribution of legendary pokemon bewtween the different
# generation. I hope that there is not a large difference in the total amount of legendary
# ax = legendary.groupby('Generation') \
#             .size() \
#             .plot(kind='bar',
#                   title='Number of Pokemon Per Generation')
# ax.set_xlabel('Generation')
# ax.set_ylabel('Number of Pokemon')
# plt.show()


# Viewing the distribution of the Attack, Defense, and Speed stats. I want to check for outliers as this might also skew
# the data if there are too many
# sns.boxplot(data=legendary,x='Generation',y='Attack')\
#             .set(title='Attack Distribution by Generation')
# plt.show()
# sns.boxplot(data=legendary,x='Generation',y='Defense')\
#             .set(title='Defense Distribution by Generation')
# plt.show()
# sns.boxplot(data=legendary,x='Generation',y='Speed')\
#             .set(title='Speed Distribution by Generation')
# plt.show()

# Viewing the correlation between all stats. One thing to note is while my main focus is Attack, Defense, and Speed, the
# "special statistics" are something to also consider when choosing a team
sns.pairplot(legendary,vars=['Attack','Defense','Sp. Atk','Sp. Def','Speed'],hue='Generation')
# plt.show()

pokemon_corr = legendary[['HP','Attack','Defense','Sp. Atk','Sp. Def','Speed']].corr()
sns.heatmap(pokemon_corr,annot=True)
plt.title('Correlation Between Different Pokemon Stats')
# plt.show()

# Now that we seen the amount of legendary pokemon per generation, the distribution between the main stats, as well as their correlation to the special stats
# I want to see which types on average have the highest amongst the three main stats
# avg_atk = legendary['Attack'].mean().round(2)
# pokemon_agg = legendary.groupby('Type 1') \
#                      .agg(Attack = ('Attack','mean')) \
#                      .round(2) \
#                      .reset_index()
# pokemon_agg['Hex Color'] = ['#A6B91A','#705746','#6F35FC','#F7D02C','#D685AD','#C22E28','#EE8130','#A98FF3','#735797','#7AC74C','#E2BF65','#96D9D6','#A8A77A','#A33EA1','#F95587','#B6A136','#B7B7CE','#6390F0']
# plt.barh(pokemon_agg['Type 1'],pokemon_agg['Attack'],color=pokemon_agg['Hex Color'])
# plt.axvline(x=avg_atk,color='black',linestyle='dashed',label='Average Attack Stat')
# plt.xlabel('Pokemon Type')
# plt.ylabel('Attack Stat')
# plt.legend(loc='upper right')
# plt.show()
#
# avg_def = legendary['Defense'].mean().round(2)
# pokemon_agg = legendary.groupby('Type 1') \
#                      .agg(Defense = ('Defense','mean')) \
#                      .round(2) \
#                      .reset_index()
# pokemon_agg['Hex Color'] = ['#A6B91A','#705746','#6F35FC','#F7D02C','#D685AD','#C22E28','#EE8130','#A98FF3','#735797','#7AC74C','#E2BF65','#96D9D6','#A8A77A','#A33EA1','#F95587','#B6A136','#B7B7CE','#6390F0']
# plt.barh(pokemon_agg['Type 1'],pokemon_agg['Defense'],color=pokemon_agg['Hex Color'])
# plt.axvline(x=avg_def,color='black',linestyle='dashed',label='Average Defense Stat')
# plt.xlabel('Pokemon Type')
# plt.ylabel('Defense Stat')
# plt.legend(loc='upper right')
# plt.show()
#
# avg_spd = legendary['Speed'].mean().round(2)
# pokemon_agg = legendary.groupby('Type 1') \
#                      .agg(Speed = ('Speed','mean')) \
#                      .round(2) \
#                      .reset_index()
# pokemon_agg['Hex Color'] = ['#A6B91A','#705746','#6F35FC','#F7D02C','#D685AD','#C22E28','#EE8130','#A98FF3','#735797','#7AC74C','#E2BF65','#96D9D6','#A8A77A','#A33EA1','#F95587','#B6A136','#B7B7CE','#6390F0']
# plt.barh(pokemon_agg['Type 1'],pokemon_agg['Speed'],color=pokemon_agg['Hex Color'])
# plt.axvline(x=avg_spd,color='black',linestyle='dashed',label='Average Speed Stat')
# plt.xlabel('Pokemon Type')
# plt.ylabel('Speed Stat')
# plt.legend(loc='upper right')
# plt.show()