"""
Script to get the longitude & latitude data from the city name
@author: nasekyung
"""

from geopy.geocoders import Nominatim
import pandas as pd


def GeoCoding(path):
    filepath = path
    Geolist = []
    Geodata = pd.read_csv(filepath, "rb", delimiter = ",")
    for i in range(len(Geodata)):
        geolocator = Nominatim(user_agent="specify_your_app_name_here", timeout=1000)
        city_name = str(Geodata["Zone"][i])
        County = str(Geodata["Borough"][i])
        State = "USA"
        print(city_name, County)
        
        if geolocator.geocode(city_name) != None:
            location = geolocator.geocode(str(city_name + " " + County + " "+ State))
            print(location)
            if location:
                latitude = location.latitude
                longitude = location.longitude
                
                Geolist.append([i+1, latitude, longitude, location])
            else:
                Geolist.append([i+1, 0,0, location])               
            
        else:
            Geolist.append([i+1, 0,0, location])
    
    return Geolist
    

GeoInfoList = GeoCoding("your_filepath/taxi+_zone_lookup.csv")
test_GeoInfo = pd.DataFrame(GeoInfoList, columns = ["Index", "Latitude", "Longtitude"])

test_GeoInfo.to_csv("your_filepath/GeoInfo_test3.csv")
