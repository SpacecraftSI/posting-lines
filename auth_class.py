# auth_class.py
# class for server authentication and login

class login:
    # Postgresql/Postgis database login details
    db = 'db'
    host = 'localhost'
    port = '0000'
    schem = 'public'

    user = 'username'
    pw = 'password'

    inputDb = 'inputDb'
    outputDb = 'outputDb' #currently redundant as script uses inputDb for output

    tempDb = 'temp'

    # loading csv into test database
    loaderDb = 'loaderDb'
