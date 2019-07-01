import pandas as pd
import sqlite3
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import requests
import pymongo
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
    Total_Games['Total_Games'] = Total_Games[1] + Total_Games[2]
    Total_Games = Total_Games.set_index(0)
    
    team_total = Total_Games.loc[team_name, 'Total_Games']
    
    #subtract total from draws and wins function to find losses 
    total_losses = team_total - (total_draws(team_name) + total_wins(team_name))
    
    return total_losses


def histogram(team_name):
    import seaborn as sns
    import matplotlib.pyplot as plt
    
  
    wins = total_wins(team_name)
    losses = total_losses(team_name)

    index = [f'{team_name}']

    colors = ['lightBlue', 'lavender']
    df = pd.DataFrame({'wins': wins, 'losses': losses}, index=index)
    ax = df.plot.bar(rot=0, color = colors)
    return


class scrape_dark_sky():
    '''makes a dictionary of the rain for each day a game was played'''
    
    
    def __init__(self):
        self.latitude = 52.5200
        self.longitude = 13.4050
        self.secret_key = '6451ebd4d55c9eaa573cda7e94ad37d0'
        self.temp = {}
    
    def unix_list(self):
        c.execute('''SELECT * FROM matches
                     WHERE Season == 2011
                     ''')
        df_matches = pd.DataFrame(c.fetchall())
        dates = df_matches[3]
        review = pd.to_datetime(pd.Series(dates))
        df_matches[3] = review
        dates_UNIX = list(df_matches[3].astype(np.int64) // 10**9)
        return dates_UNIX
    
    def get_rain(self):
        
        
        for date in scrape_dark_sky().unix_list(): #limits to 5 take out for real run
            date = date
            URL = 'https://api.darksky.net/forecast/{}/{},{},{}?exclude=currently,flags,hourly,minutely,alerts'.format(self.secret_key, self.latitude, self.longitude, date)
        
            resp = requests.get(URL)
            r = resp.json()
            rain_for_day = r['daily']['data'][0]['precipIntensity']
            
            if rain_for_day == 0:
                rain_for_day = False 
            else:
                rain_for_day = True
                
            date = datetime.utcfromtimestamp(date).strftime('%Y-%m-%d')
            self.temp.update({date : rain_for_day})
        
        return self.temp
     

class soccer_weather():
    
    def __init__(self):
        self.date_dict = {'2012-03-31': True, '2011-12-11': False, '2011-08-13': False,
 '2011-11-27': False,
 '2012-02-18': False,
 '2012-01-20': True,
 '2012-02-04': False,
 '2012-04-21': False,
 '2011-09-18': True,
 '2011-10-23': True,
 '2011-10-01': False,
 '2012-03-03': False,
 '2011-08-27': True,
 '2012-03-17': False,
 '2011-11-06': True,
 '2012-05-05': True,
 '2012-04-11': False,
 '2011-12-17': True,
 '2012-02-03': False,
 '2011-10-29': False,
 '2012-01-22': True,
 '2011-12-03': True,
 '2012-04-14': False,
 '2012-03-25': False,
 '2012-03-10': False,
 '2012-04-07': False,
 '2011-11-19': False,
 '2011-10-14': False,
 '2011-09-24': False,
 '2012-04-28': False,
 '2011-12-18': True,
 '2012-03-02': False,
 '2012-03-16': False,
 '2012-02-17': True,
 '2011-08-06': True,
 '2011-11-04': False,
 '2011-09-16': False,
 '2011-07-15': False,
 '2012-05-06': True,
 '2012-02-11': False,
 '2011-10-02': False,
 '2012-03-30': False,
 '2011-10-22': False,
 '2011-08-26': True,
 '2011-08-07': True,
 '2012-02-24': True,
 '2011-07-17': True,
 '2012-02-12': False,
 '2011-08-22': False,
 '2011-09-09': True,
 '2012-04-01': False,
 '2011-09-25': False,
 '2012-04-20': False,
 '2011-11-25': True,
 '2012-03-09': False,
 '2011-11-07': False,
 '2011-09-10': True,
 '2011-08-21': False,
 '2011-09-26': False,
 '2012-03-26': False,
 '2011-10-30': True,
 '2012-02-13': False,
 '2012-02-26': False,
 '2011-12-19': False,
 '2012-03-11': False,
 '2011-11-26': True,
 '2012-04-23': False,
 '2011-12-16': True,
 '2011-11-05': True,
 '2012-04-10': False,
 '2012-04-15': False,
 '2012-02-05': False,
 '2012-03-04': False,
 '2011-09-17': False,
 '2012-01-21': False,
 '2012-03-18': False,
 '2012-04-22': False,
 '2011-08-20': False,
 '2012-02-10': False,
 '2012-02-25': False,
 '2011-12-10': True,
 '2012-01-29': False,
 '2011-10-16': True,
 '2011-08-05': True,
 '2012-03-23': False,
 '2011-09-23': False,
 '2012-01-28': False,
 '2011-10-15': False,
 '2012-03-24': False,
 '2011-11-18': True,
 '2012-04-29': False,
 '2011-12-09': True,
 '2012-04-08': False,
 '2012-03-12': True,
 '2011-07-23': True,
 '2011-10-28': False,
 '2012-01-27': False,
 '2011-12-04': False,
 '2011-09-11': True,
 '2011-09-30': False,
 '2012-02-19': True,
 '2012-04-13': False,
 '2011-10-21': False,
 '2011-08-14': True,
 '2011-09-12': True,
 '2011-07-18': True,
 '2011-08-08': True,
 '2011-07-24': False,
 '2011-12-13': True,
 '2011-08-28': False,
 '2011-08-19': True,
 '2012-03-14': False,
 '2011-08-12': True,
 '2012-03-05': False,
 '2011-12-02': True,
 '2011-09-19': True,
 '2011-07-25': False,
 '2012-04-02': True,
 '2012-02-06': False,
 '2011-11-21': False,
 '2011-07-22': True,
 '2011-08-29': True,
 '2012-04-05': False,
 '2011-11-28': False,
 '2011-12-12': True,
 '2011-11-20': False,
 '2012-02-27': True,
 '2011-07-16': False,
 '2011-10-31': False,
 '2012-02-20': False,
 '2011-10-17': False,
 '2011-12-05': False,
 '2012-04-16': False,
 '2011-10-03': False,
 '2012-03-19': False,
 '2011-08-15': True,
 '2011-12-20': False,
 '2011-12-21': True,
 '2011-12-22': True,
 '2011-12-26': True,
 '2011-12-27': False,
 '2011-12-30': True,
 '2011-12-31': False,
 '2012-01-01': True,
 '2012-01-02': True,
 '2012-01-03': True,
 '2012-01-04': True,
 '2012-01-11': False,
 '2012-01-14': False,
 '2012-01-15': False,
 '2012-01-16': True,
 '2012-01-31': False,
 '2012-02-01': False,
 '2012-03-13': False,
 '2012-03-20': False,
 '2012-03-21': False,
 '2012-04-06': False,
 '2012-04-09': False,
 '2012-04-24': True,
 '2012-04-30': False,
 '2012-05-01': False,
 '2012-05-02': False,
 '2012-05-07': False,
 '2012-05-08': False,
 '2012-05-13': False}
        
        
    
    def total_wins_in_rain(self, team_name):
        '''returns the percentage of wins a team had for games they played in which it was raining'''

        #away team wins
        c.execute('''SELECT Date, AwayTeam FROM MATCHES 
                     WHERE FTR == 'A' 
                     AND Season == 2011
                     ''')
        Away_Team_Wins = pd.DataFrame(c.fetchall())

        #home team wins 
        c.execute('''SELECT Date, HomeTeam FROM MATCHES 
                     WHERE FTR == 'H' 
                     AND Season == 2011''')
        Home_Team_Wins = pd.DataFrame(c.fetchall())

        #combine tables
        wins_and_date = Away_Team_Wins.append(Home_Team_Wins)
        #wins_and_date


        #turn it into DF
        rain_df = pd.DataFrame.from_dict(self.date_dict, orient = 'index').reset_index()
        #rain_df

        #combine total wins and rain table in order to find total amount of wins in the rain 
        rain_wins = pd.merge(left=wins_and_date, right = rain_df, left_on = 0, right_on = 'index')
        rain_wins = rain_wins.drop(['key_0', '0_x'], axis = 1)
        rain_wins.columns = ['team', 'Date', 'rain?']
        rain_wins = rain_wins[rain_wins['rain?'] == True]
        
        try: 
            return rain_wins.team.value_counts().loc['{}'.format(team_name)]
        except: 
            return 0.0 


    def total_games_in_rain(self, team_name):
        '''takes in team name and returns the total amount of games played in rain'''

        c.execute('''select date, HomeTeam, AwayTeam FROM matches
                 WHERE Season == 2011
                 ORDER BY Date
                ''')

        df_matches = pd.DataFrame(c.fetchall())
        df_matches.columns = ['Date','HomeTeam','AwayTeam']

        #Adding column of whether it not it rained on that date
        
       
        df_matches['rain'] = df_matches.Date.apply(lambda x: self.date_dict[x])
       

        #filtering for just true values in the rain column
        games_in_rain = df_matches[df_matches['rain'] == True]

        total_games = games_in_rain.AwayTeam.value_counts().loc['{}'.format(team_name)] + games_in_rain.HomeTeam.value_counts().loc['{}'.format(team_name)]

        return total_games
    
    def rain_win_percentage(self, team_name):
        return round((self.total_wins_in_rain(team_name) / self.total_games_in_rain(team_name)) * 100, 2) 
    
    
    
class mongo_handler():
    

    
    def __init__(self):
        self.myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017")
        self.mydb = self.myclient['database']
        self.mycollection = self.mydb['team_info']
        self.soccer = soccer_weather()
        
        
    def info_to_dict(self):
        
        list_of_dicts = []
        
        for team in total_teams():
            team_dict = {}
            team_dict['name'] = team
            team_dict['total_goals_2011'] = str(total_goals(team))
            team_dict['total_wins_2011'] = str(total_wins(team))
            team_dict['total_rain_wins'] = str(self.soccer.rain_win_percentage(team))
            list_of_dicts.append(team_dict)
            
            
        return list_of_dicts
    
    

    def insert_into_mongos(self):
        
        results_2 = mycollection.insert_many(self.info_to_dict)
        return results_2

    
    
    
    
    
    
    
    