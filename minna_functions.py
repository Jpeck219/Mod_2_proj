#1. take info from SQL database and put it into Mongo 
#2. scrape darksky API based on day or time, all for Berlin, Germany 
# The team's win percentage on days where it was raining during games in the 2011 season.

import pandas as pd
import sqlite3
conn = sqlite3.connect('database.sqlite')
c = conn.cursor()

def total_teams():
    c.execute('''SELECT AwayTeam FROM Matches
             WHERE Season == 2011
             Group By 1''')
    
    Total_Teams = pd.DataFrame(c.fetchall())
    return list(Total_Teams[0])

def total_goals(team_name):
    '''This function takes in the team name as a string and returns the total goals for the team during the 2011 season'''
    
    #create data frame for away team
    c.execute('''SELECT AwayTeam, Unique_Teams.Unique_Team_ID, sum(FTAG)
             FROM matches 
             JOIN Unique_Teams
             ON Unique_Teams.TeamName = AwayTeam
             WHERE Season == 2011
             Group By 1;''')
    df_away = pd.DataFrame(c.fetchall())
    df_away.columns = ['AwayTeam', 'Team_ID', 'Total_Away_Goals']

    #create data frame for home team 
    c.execute('''SELECT HomeTeam, Unique_Teams.Unique_Team_ID, sum(FTHG)
            FROM matches
            JOIN Unique_Teams
            ON Unique_Teams.TeamName = HomeTeam
            WHERE Season == 2011
            Group By 1;''')
    df_home = pd.DataFrame(c.fetchall())
    df_home.columns = ['Team', 'Unique_Team_ID', 'Home_Goals']
    
    #combine the two dataframes and drop redundance columns and set index to team
    df_joined = pd.concat([df_home, df_away], axis=1)
    df_joined = df_joined.drop(['AwayTeam', 'Team_ID'], axis=1, inplace= False)
    df_joined['Total_Goals'] = df_joined['Home_Goals'] + df_joined['Total_Away_Goals']
    df_joined = df_joined.set_index('Team')
    
    return df_joined.loc[team_name, 'Total_Goals']
    
    

def total_wins(team_name):
    '''This function takes in the team name as a string and returns the total wins for the team during the 2011 season'''
    
    #create dataframe for Away Team wins
    c.execute('''SELECT AwayTeam, count(FTR) FROM MATCHES 
             WHERE FTR == 'A' 
             AND Season == 2011
             Group By 1''')
    Away_Team_Wins = pd.DataFrame(c.fetchall())
    Away_Team_Wins.columns = ['Away_Team_Name', 'Away_Wins']
    
    #create dataframe for Home Team wins 
    c.execute('''SELECT HomeTeam, count(FTR) FROM MATCHES 
             WHERE FTR == 'H' 
             AND Season == 2011
             Group By 1''')
    Home_Team_Wins = pd.DataFrame(c.fetchall())
    Home_Team_Wins.columns = ['Team_Name', 'Home_Wins']
    
    #create total wins data frame
    df_total_wins = pd.concat([Home_Team_Wins, Away_Team_Wins], axis=1)
    df_Wins = df_total_wins.drop(['Away_Team_Name'], axis=1, inplace = False)
    df_Wins['Total_Wins'] = df_Wins['Home_Wins']+df_Wins['Away_Wins']
    df_Wins = df_Wins.set_index('Team_Name')
    
    
    return df_Wins.loc[team_name, 'Total_Wins']
    
    
def total_draws(team_name):
    '''this function takes in the team name as a string and returns the total draws for the team during the 2011 season'''
    
    
    c.execute('''SELECT TeamName, COUNT(FTR) 
             FROM Matches
             JOIN Teams_in_Matches 
             USING (Match_ID)
             JOIN Unique_Teams
             USING (Unique_Team_ID)
             WHERE FTR == 'D'
             AND Season == 2011
             GROUP BY 1''')
    
    df_draws = pd.DataFrame(c.fetchall())
    df_draws.columns = ['Team_Name', 'Draws']
    df_draws = df_draws.set_index('Team_Name')
    
    
    return df_draws.loc[team_name, 'Draws']

    
def total_losses(team_name):    
    '''this function returns the total losses for a team during the 2011 season'''
    
    #first find total games for the season
    c.execute('''SELECT HomeTeam , count(HomeTeam) , count(AwayTeam) FROM MATCHES  
             WHERE Season == 2011
             Group By 1''')
    Total_Games = pd.DataFrame(c.fetchall())
    Total_Games['Total_Games'] = Total_Games[1]+ Total_Games[2]
    Total_Games = Total_Games.set_index(0)
    
    team_total = Total_Games.loc[team_name, 'Total_Games']
    
    #subtract total from draws and wins function to find losses 
    total_losses = team_total - (total_draws(team_name) + total_wins(team_name))
    
    return total_losses


def histogram():
    import seaborn as sns
    import matplotlib.pyplot as plt
    
    d = []
    for team in total_teams():
        d.append((team, total_wins(team), total_losses(team)))
    
    df = pd.DataFrame(d, columns = ('Team', 'Wins', 'Losses')) 

    fig = plt.figure(figsize = (10,10))
    sns.distplot(df['Wins'], color = 'Green', bins = 15)
    sns.distplot(df['Losses'], color = 'Orange', bins = 15)
    fig.legend(labels = ['Wins', 'Losses'])
    plt.xlabel("number of wins or losses")
    plt.ylabel("percentage of total teams")
    plt.title("distribution of wins vs. losses for 2011 soccer season")
    plt.show()
    return
    
    
    
    
    
    
    
    
    
    