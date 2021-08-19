# Seleinum script to scrape station address (gu, dong)
from urllib.request import urlopen
from lxml import etree
import urllib
from retrying import retry

import math
import pandas as pd

# Scrape station addresses from 나무위키 using lxml
def scrape_address():

    def find_gu_dong(station):
        base_url = 'https://namu.wiki/w/'
        station_name = station + '역'
        station_name = station_name.replace(' ','')
        station_name = urllib.parse.quote(station_name)
        url = base_url + station_name

        header= {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
        }

        req=urllib.request.Request(url,None,header)
        
        response = urlopen(req)
        response = urllib.request.urlopen(req)

        htmlparser = etree.HTMLParser(encoding='utf-8')
        
        tree = etree.parse(response, htmlparser)
        for j in range(1,5):
            for i in range(10,20):
                tr = tree.xpath('//*[@id="app"]/div/div[2]/article/div[3]/div[2]/div/div/div[' + str(j) + ']/table/tbody/tr[' + str(i) + ']/td/div/a')
                if tr:  # Find the address component
                    if tr[0].text == '서울특별시':
                        gu = tree.xpath('//*[@id="app"]/div/div[2]/article/div[3]/div[2]/div/div/div[' + str(j) + ']/table/tbody/tr[' + str(i) + ']/td/div/a')[1].text
                        dong = tree.xpath('//*[@id="app"]/div/div[2]/article/div[3]/div[2]/div/div/div[' + str(j) + ']/table/tbody/tr[' + str(i) + ']/td/div/a')[-1].text
                        return [gu, dong]
                    elif tr[0].text == '경기도':
                        try:
                            gu_1 = tree.xpath('//*[@id="app"]/div/div[2]/article/div[3]/div[2]/div/div/div[' + str(j) + ']/table/tbody/tr[' + str(i) + ']/td/div/a')[1].text
                            gu_2 = tree.xpath('//*[@id="app"]/div/div[2]/article/div[3]/div[2]/div/div/div[' + str(j) + ']/table/tbody/tr[' + str(i) + ']/td/div/a')[2].text
                            gu = gu_1 + ' ' + gu_2
                            dong = tree.xpath('//*[@id="app"]/div/div[2]/article/div[3]/div[2]/div/div/div[' + str(j) + ']/table/tbody/tr[' + str(i) + ']/td/div/a')[-1].text
                            if dong[-1] != '동':
                                dong = tree.xpath('//*[@id="app"]/div/div[2]/article/div[3]/div[2]/div/div/div[' + str(j) + ']/table/tbody/tr[' + str(i) + ']/td/div/a')[-1].tail.split()[-1][1:-1]
                            return [gu, dong]
                        except IndexError:
                            return ['','']
                        except TypeError:
                            return ['','']
        return ['','']

    base_path = 'data/train/'
    df_stations = pd.read_csv(base_path + 'gu_dong_addresses.csv', index_col=False)

    for index, row in df_stations.iterrows():
        if index > 512:
            station_name = row['역명']
            gu_dong_list = find_gu_dong(station_name)
            df_stations.loc[index, '구'] = gu_dong_list[0]
            df_stations.loc[index, '동'] = gu_dong_list[1]
            print(station_name, gu_dong_list[0], gu_dong_list[1])
            df_stations.to_csv(base_path + 'gu_dong_addresses.csv', index=False)
        
# Filter null / faulty  rows for further processing
def filter_faulty():
    base_path = './data/train/'
    df_address = pd.read_csv(base_path + 'gu_dong_addresses_utf.csv', encoding='utf-8-sig')
    
    # Filter null / faulty  rows for further processing
    for index, row in df_address.iterrows():
        if (not isinstance(row['구'], str)) or (not isinstance(row['동'], str)):
            df_address.loc[index, 'Correct'] = False
        elif row['구'][-1] != '구':
            df_address.loc[index, 'Correct'] = False
        elif row['동'][-1] != '동':
            df_address.loc[index, 'Correct'] = False
        else:
            df_address.loc[index, 'Correct'] = True


    df_address.to_csv(base_path + 'gu_dong_address_utf.csv', index=False, encoding='utf-8-sig')

# Map station addresses to monthly_all csv
def map_station_address():
    base_path = './data/train/'

    df_target = pd.read_csv(base_path + 'monthly_all.csv', encoding='utf-8-sig')
    df_address = pd.read_csv(base_path + 'gu_dong_addresses_utf_checked.csv', encoding='utf-8-sig')

    station_list = df_target['역명'].unique()

    for index, row in df_address.iterrows():
        # Check if station exists in monthly_all.csv    
        if row['역명'] in station_list:
            df_target.loc[df_target['역명'] == row['역명'], '구'] = row['구']
            df_target.loc[df_target['역명'] == row['역명'], '동'] = row['동']

    df_target.to_csv(base_path + 'monthly_all_address.csv', encoding='utf-8-sig')
    

if __name__ == '__main__':
    map_station_address()