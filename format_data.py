import pandas as pd
import datetime

def main():
    base_path = 'data/train/'
    for year in range(2015, 2022):
        for month in range(1, 13):
            print(year, month)
            if len(str(month)) == 1:
                month_str = '0' + str(month)
            else:
                month_str = str(month)
            
            date_str = str(year) + month_str
            
            if date_str == '202104':
                break
            
            final_path = base_path + 'CARD_SUBWAY_MONTH_' +  date_str + '.csv'
            target_path = base_path + 'formatted/CARD_SUBWAY_MONTH_' + date_str + '.csv'

            df = pd.read_csv(final_path)
            # Reformat data to datetime

            df['date'] = df['사용일자'].apply(lambda d: datetime.datetime.strptime(str(d), '%Y%m%d').strftime('%m/%d/%Y'))
            df.drop('사용일자', inplace=True, axis=1)
            df = df[['date', '노선명', '역명', '승차총승객수', '하차총승객수', '등록일자']]
            df.to_csv(final_path,encoding='utf-8-sig', index=False)

def remove_index():
    base_path = 'data/train/'
    for year in range(2015, 2022):
        for month in range(1, 13):
            print(year, month)
            if len(str(month)) == 1:
                month_str = '0' + str(month)
            else:
                month_str = str(month)
            
            date_str = str(year) + month_str
            
            if date_str == '202005':
                break
            
            final_path = base_path + 'CARD_SUBWAY_MONTH_' +  date_str + '.csv'
            target_path = base_path + 'formatted/CARD_SUBWAY_MONTH_' + date_str + '.csv'

            df = pd.read_csv(final_path, index_col=0)
            df.to_csv(final_path, index=False)

# Create csv for each month by averaging the on/off count for each station
def monthly_mean():
    base_path = 'data/train/raw_data/'
    for year in range(2015, 2022):
        for month in range(1, 13):
            print(year, month)
            if len(str(month)) == 1:
                month_str = '0' + str(month)
            else:
                month_str = str(month)
            
            date_str = str(year) + month_str
            
            if date_str == '202104':
                break
            
            final_path = base_path + 'CARD_SUBWAY_MONTH_' +  date_str + '.csv'
            target_path = base_path + 'monthly_mean/CARD_SUBWAY_MONTH_' + date_str + '.csv'

            df = pd.read_csv(final_path)
            station_names = df['역명'].unique()

            mean_df = pd.DataFrame(columns = ['date','노선명','역명','승차총승객수','하차총승객수'])
            # Loop through each station and calculate mean value
            for station in station_names:
                df_station = df[df['역명'] == station]
                on_mean = df_station['승차총승객수'].mean()
                off_mean =df_station['하차총승객수'].mean()

                data = {
                    'date': datetime.datetime(year, month, 1),
                    '노선명': df_station['노선명'].unique()[0],
                    '역명': station,
                    '승차총승객수': on_mean,
                    '하차총승객수': off_mean
                }
                mean_df = mean_df.append(data, ignore_index=True)

            mean_df.columns = ['date','노선명','역명','승차총승객수','하차총승객수']
            mean_df.to_csv(target_path, encoding='utf-8-sig', index=False)

# Congregate monthly data into a single csv for each year.
def congregate_yearly():
    base_path = 'data/train/'

    for year in range(2015, 2022):
        year_df = pd.DataFrame(columns = ['date','노선명','역명','승차총승객수','하차총승객수'])

        
        for month in range(1, 13):
            print(year, month)

            # Building CSV path
            if len(str(month)) == 1:
                month_str = '0' + str(month)
            else:
                month_str = str(month)
            date_str = str(year) + month_str
            
            if date_str == '202104':
                break
            
            final_path = base_path + 'monthly_mean/CARD_SUBWAY_MONTH_' +  date_str + '.csv'
            target_path = base_path + 'yearly_congregated/CARD_SUBWAY_MONTH_' + str(year)
            
            month_df = pd.read_csv(final_path)
            
            year_df = year_df.append(month_df)
        
        year_df.to_csv(target_path + '.csv')
        year_df.to_pickle(target_path + '.pkl')

# Calculate mean for each year, and congregate into a single csv
def yearly_mean():
    base_path = 'data/train/'
    for year in range(2015, 2022):
        print(year)

        final_path = base_path + 'yearly_congregated/CARD_SUBWAY_MONTH_' +  str(year) + '.pkl'
        target_path = base_path + 'CARD_SUBWAY'

        df = pd.read_pickle(final_path)
        station_names = df['역명'].unique()

        mean_df = pd.DataFrame(columns = ['date','노선명','역명','승차총승객수','하차총승객수'])
        # Loop through each station and calculate mean value
        for station in station_names:
            df_station = df[df['역명'] == station]
            on_mean = df_station['승차총승객수'].mean()
            off_mean =df_station['하차총승객수'].mean()

            data = {
                'date': datetime.datetime(year, month, 1),
                '노선명': df_station['노 선명'].unique()[0],
                '역명': station,
                '승차총승객수': on_mean,
                '하차총승객수': off_mean
            }
            mean_df = mean_df.append(data, ignore_index=True)

        mean_df.columns = ['date','노선명','역명','승차총승객수','하차총승객수']
        mean_df.to_csv(target_path, encoding='utf-8-sig', index=False)     

# Congregate monthly values for 2015~2020
def monthly_total():
    
    base_path = 'data/train/'
    final_df = pd.DataFrame(columns = ['date','노선명','역명','승차총승객수','하차총승객수'])
    
    for year in range(2015, 2022):

        df_year = pd.read_pickle(base_path + 'yearly_congregated/CARD_SUBWAY_MONTH_' + str(year) + '.pkl')
        final_df = final_df.append(df_year)

# Calculate means for each year
def yearly_total():
    base_path = 'data/train/'
    df = pd.read_csv(base_path + 'monthly_all_address.csv', encoding='utf-8-sig', index_col=0)
    df_yearly = pd.DataFrame(columns=['date','노선명','역명','승차총승객수','하차총승객수','구','동'])
    station_list = df['역명'].unique()

    d = {
        'date':[],
        '역명':[],
        '승차총승객수':[],
        '하차총승객수':[],
        '구':[],
        '동':[]
    }

    for year in range (2015, 2022):
        df_year = df[str(year) + '-01-01':str(year) + '-12-31']
        df_year_mean = df_year.groupby(['역명','구','동'], as_index=False).mean()
        for index, row in df_year_mean.iterrows():
            d['date'].append(datetime.datetime(year, 1, 1))
            d['역명'].append(row['역명'])
            d['승차총승객수'].append(row['승차총승객수'])
            d['하차총승객수'].append(row['하차총승객수'])
            d['구'].append(row['구'])
            d['동'].append(row['동'])
    
    df_final = pd.DataFrame(data=d)
    df_final.to_csv(base_path + 'yearly_all_address.csv', encoding='utf-8-sig')

# Calculate yearly change for each station
def yearly_change():
    base_path = './data/train/'
    df = pd.read_csv(base_path + 'yearly_all_address.csv', encoding='utf-8-sig')

    d = {
        'date': [],
        '역명': [],
        '승차총승객수': [],
        '구': [],
        '동': [],
        'change': []
    }

    # Calculate % change per year
    for station in df['역명']:
        df_station = df.loc[df['역명'] == station]
        df_station['shift'] = df_station['승차총승객수'].shift(1)
        df_station['change'] = df_station['승차총승객수'].subtract(df_station['shift']).div(df_station['shift'])
        
        # insert values for rows in each date
        for index, row in df_station.iterrows():
            d['date'].append(row['date'])
            d['역명'].append(row['역명'])
            d['승차총승객수'].append(row['승차총승객수'])
            d['구'].append(row['구'])
            d['동'].append(row['동'])
            d['change'].append(row['change'])
    
    
    df_change = pd.DataFrame(data=d)
    df_change.to_csv(base_path + 'yearly_change_address.csv', encoding='utf-8-sig', index=False)

# Remove values in parens
def remove_parens():
    base_path = 'data/train/'
    df_address = pd.read_csv(base_path + 'monthly_all.csv')

    def parse_name(station):
        if '(' in station:
            station = station.split('(')[0]
        return station

    df_address['역명'] = df_address['역명'].apply(lambda station: parse_name(station))
    df_address.to_csv(base_path + 'monthly_all.csv', encoding='utf-8-sig',index=False)

# Average monthly train users by dong
def gu_dong_mean():

    base_path = './data/train/'

    df = pd.read_csv(base_path + 'monthly_all_address.csv')

    df_dong = df.groupby(['date','동', '구'], as_index=False).mean()
    df_dong.to_csv(base_path + 'gu_dong_address_numbers.csv', )


if __name__ == "__main__":
    gu_dong_mean()
