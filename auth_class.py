# auth_class.py
# class for server authentication and login

class login:
    # Postgresql/Postgis database login details
    db = 'eccc-data'
    host = 'localhost'
    port = '5432'
    schem = 'public'

    user = 'postgres'
    pw = 'postgres'

    inputDb = ['segments_2023_07','segments_2023_08','segments_2023_09','segments_2023_10','segments_2023_11','segments_2023_12']

    tempDb = 'temp'

    # loading csv into test database
    loaderDb = 'loaderDb'
