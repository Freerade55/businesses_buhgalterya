
import base64
import fileinput
import urllib.request
import json
from flask import request, Flask, send_file, jsonify, g, render_template, redirect, make_response, url_for, abort, \
    send_from_directory, flash
from flask_cors import CORS
# from file import connect
import pymysql
import webbrowser
import io
import PIL.Image as Image
from base64 import b64decode as dec64
from base64 import b64encode as enc64
import os
from werkzeug.utils import secure_filename
from datetime import *
import os
import requests
from datetime import datetime
import jwt
import string
import pandas as pd


app = Flask(__name__)
CORS(app)


def connect_db():
    return pymysql.connect(
    host="mysql104.1gb.ru",
    user="gb_camera",
    password="",
    database = "gb_camera",
    cursorclass = pymysql.cursors.DictCursor
    )


def get_db():
    '''Opens a new database connection per request.'''
    if not hasattr(g, 'db'):
        g.db = connect_db()
    return g.db



@app.teardown_appcontext
def close_db(error):
    '''Closes the database connection at the end of request.'''
    if hasattr(g, 'db'):
        g.db.close()



### сейв на момент 18.02.2022



@app.route('/autorization', methods=['POST'])
def autorization():
    request_data = request.get_json()

    user_request = {
        'loggin': request_data['loggin'],
        'password': request_data['password'],
    }


    cursor = get_db().cursor()
    cursor.execute(
        """SELECT * FROM users;""")
    cursor.close()
    rows = cursor.fetchall()


    for i in rows:
        if user_request['loggin'] == i['user_name'] and user_request['password'] == i['password']:
            cursor = get_db().cursor()
            cursor.execute(
                f"""SELECT COUNT(perevod_status) FROM perevody WHERE perevod_status = 'false' AND 
                user_get = '{i['id']}';""")
            get = cursor.fetchall()
            print(get)


            return jsonify({'id': i['id'], 'user_name': i['user_name'], 'balance': i['balance'],
                            'money': i['money'], 'my_money': i['my_money'], 'status_count': get[0]['COUNT(perevod_status)'],
                            "status": i['my_businesses']})
    return jsonify('Неверный логин или пароль')
@app.route('/my_profile', methods=['POST'])
def my_profile():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id']

    }
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_name, my_businesses FROM users WHERE id = '{user_request['id']}';""")
    get = cursor.fetchall()


    return jsonify(get)




@app.route('/prihod/business', methods=['GET'])
def prihod_business():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM a_businesses WHERE status <> 'false';""")
    businesses = cursor.fetchall()

    return jsonify(businesses)
@app.route('/add_business', methods=['POST'])
def add_business():
    request_data = request.get_json()
    user_request = {
        'add_business': request_data['add_business']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""INSERT INTO spisok (businesses, businesses_status) VALUES ('{user_request['add_business']}', 
        '{user_request['add_business']}/true');""")
    g.db.commit()

    cursor.execute(
        f"""SELECT businesses FROM spisok;""")
    get = cursor.fetchall()

    arr = []
    new = [i['businesses'] for i in get]
    new2 = [False for i in get]
    arr.extend([new, new2])

    return jsonify(arr)
@app.route('/prihod_istochnik', methods=['POST'])
def prihod_istochnik():
    request_data = request.get_json()
    user_request = {
        'businesses': request_data['businesses']
    }
    print(user_request)
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT istochnik_prihoda FROM spisok WHERE businesses = '{user_request['businesses']}';""")
    get = cursor.fetchall()


    res = []
    arr = get[0]['istochnik_prihoda'].split(',')
    status = [False for i in arr]
    res.extend([arr,status])


    return jsonify(res)
@app.route('/add_istochnik', methods=['POST'])
def add_istochnik():
    request_data = request.get_json()
    user_request = {
        'businesses': request_data['businesses'],
        'istochnik': request_data['istochnik']


    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT istochnik_prihoda, istochnik_prihoda_status FROM spisok WHERE businesses = '{user_request['businesses']}';""")
    get = cursor.fetchall()


    if get[0]['istochnik_prihoda']:
        get[0]['istochnik_prihoda'] += ',' + user_request['istochnik']
    else:
        get[0]['istochnik_prihoda'] += user_request['istochnik']


    if get[0]['istochnik_prihoda_status']:
        get[0]['istochnik_prihoda_status'] += '/' + user_request['istochnik'] + '|' + 'true'
    else:
        get[0]['istochnik_prihoda_status'] += user_request['istochnik'] + '|' + 'true'








    cursor.execute(
        f"""UPDATE spisok SET istochnik_prihoda = '{get[0]['istochnik_prihoda']}',
        istochnik_prihoda_status = '{get[0]['istochnik_prihoda_status']}'
        WHERE businesses = '{user_request['businesses']}'""")
    g.db.commit()


    cursor.execute(
        f"""SELECT istochnik_prihoda FROM spisok WHERE businesses = '{user_request['businesses']}';""")
    get = cursor.fetchall()

    res = []
    arr = get[0]['istochnik_prihoda'].split(',')
    status = [False for i in arr]
    res.extend([arr, status])

    return jsonify(res)
@app.route('/prihod_position', methods=['POST'])
def prihod_position():
    request_data = request.get_json()

    user_request = {
        'businesses': request_data['businesses'],
        'istochnik_prihoda': request_data['istochnik_prihoda']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT positions_prihod FROM spisok WHERE businesses = '{user_request['businesses']}';""")
    get = cursor.fetchall()
    print(get)

    if get[0]['positions_prihod'] == '':
        return jsonify(None)
    else:

        get = get[0]['positions_prihod'].split('/')

        arr = []
        for i in get:
            i = {i.split(':')[0]: [[j.split('-')[0], 'false'] for j in i.split(':')[1].split('|') if j.split('-')[1] != 'false']}
            for e in i:
                if e == user_request['istochnik_prihoda']:
                    if i[e]:
                        for k in i[e]:
                            arr.append(k[0])

        res = []
        status = [False for i in arr]
        res.extend([arr, status])
        return jsonify(res)
@app.route('/add_position', methods=['POST'])
def add_position():
    request_data = request.get_json()

    user_request = {
        'businesses': request_data['businesses'],
        'istochnik_prihoda': request_data['istochnik_prihoda'],
        'position': request_data['position']
    }



    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT positions_prihod FROM spisok WHERE businesses = '{user_request['businesses']}';""")
    get = cursor.fetchall()


    trig = 0
    if get[0]['positions_prihod']:
        get = get[0]['positions_prihod'].split('/')
        arr = [i.split(':') for i in get]

        for i in arr:
            if i[0] == user_request['istochnik_prihoda']:
                i[1] += f'''|{user_request['position']}-true'''
                trig += 1


        if trig == 0:
            arr.append([user_request['istochnik_prihoda'], f'''{user_request['position']}-true'''])

        new = []
        for j in arr:
            j = ':'.join(j)
            new.append(j)
        add = '/'.join(new)


        cursor.execute(
            f"""UPDATE spisok SET positions_prihod = '{add}'
                     WHERE businesses = '{user_request['businesses']}'""")
        g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT positions_prihod FROM spisok WHERE businesses = '{user_request['businesses']}';""")
        get = cursor.fetchall()




        get = get[0]['positions_prihod'].split('/')

        arr = []
        for i in get:
            i = {i.split(':')[0]: [[j.split('-')[0], 'false'] for j in i.split(':')[1].split('|') if
                                   j.split('-')[1] != 'false']}
            for e in i:
                if e == user_request['istochnik_prihoda']:
                    if i[e]:
                        for k in i[e]:
                            arr.append(k[0])

        res = []
        status = [False for i in arr]
        res.extend([arr, status])
        return jsonify(res)




    else:
        get[0]['positions_prihod'] = f'''{user_request['istochnik_prihoda']}:{user_request['position']}-true'''

        cursor.execute(
            f"""UPDATE spisok SET positions_prihod = '{get[0]['positions_prihod']}'
              WHERE businesses = '{user_request['businesses']}'""")
        g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT positions_prihod FROM spisok WHERE businesses = '{user_request['businesses']}';""")
        get = cursor.fetchall()

        print(get)

        get = get[0]['positions_prihod'].split('/')

        arr = []
        for i in get:
            i = {i.split(':')[0]: [[j.split('-')[0], 'false'] for j in i.split(':')[1].split('|') if
                                   j.split('-')[1] != 'false']}
            for e in i:
                if e == user_request['istochnik_prihoda']:
                    if i[e]:
                        for k in i[e]:
                            arr.append(k[0])

        res = []
        status = [False for i in arr]
        res.extend([arr, status])
        return jsonify(res)
@app.route('/prihod_enter', methods=['POST'])
def prihod_enter():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id'],
        'sum': request_data['sum'],
        'time': request_data['time'],
        'businesses': request_data['businesses'],
        'istochnik_prihoda': request_data['istochnik_prihoda'],
        'position': request_data['position']

    }





    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, money FROM users WHERE id = '{user_request['id']}';""")
    get = cursor.fetchall()
    get[0]['balance'] += user_request['sum']
    get[0]['money'] += user_request['sum']

    cursor = get_db().cursor()
    cursor.execute(
        f"""UPDATE users SET balance = '{get[0]['balance']}', money = '{get[0]['money']}' WHERE id = '{user_request['id']}';""")
    g.db.commit()



    cursor.execute(
    f"""INSERT INTO operations (user_id, summa, data, operation_type, business, istochnik, position)
    VALUES ('{user_request['id']}', '{user_request['sum']}', '{user_request['time']}', 'Приход',
    '{user_request['businesses']}', '{user_request['istochnik_prihoda']}', '{user_request['position']}');""")
    g.db.commit()


    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, (SELECT COUNT(perevody.perevod_status) 
          FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = '{user_request['id']}') AS perevod_status FROM users WHERE id = {user_request['id']};""")

    get = cursor.fetchall()

    return jsonify({'id': user_request['id'], 'user_name': get[0]['user_name'], 'balance': get[0]['balance'],
                    'money': get[0]['money'], 'my_money': get[0]['my_money'], 'status_count': get[0]['perevod_status'],
                    "status": get[0]['my_businesses']}, {'id': user_request['id'], 'sum': user_request['sum'],
                    'time': user_request['time'], 'businesses': user_request['businesses'],
                    'istochnik_prihoda':user_request['istochnik_prihoda'], 'position': user_request['position'],
                    'operation_type': 'Приход'})


@app.route('/rashod_business', methods=['GET'])
def rashod_business():


    cursor = get_db().cursor()
    cursor.execute(
            f"""SELECT businesses FROM spisok;""")
    get = cursor.fetchall()

    arr = []
    new = [i['businesses'] for i in get]
    new2 = [False for i in get]
    arr.extend([new, new2])


    return jsonify(arr)
@app.route('/rashod_cel', methods=['POST'])
def rashod_cel():
    request_data = request.get_json()
    user_request = {
        'businesses': request_data['businesses']
    }
    print(user_request)
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT cel_rashoda FROM spisok WHERE businesses = '{user_request['businesses']}';""")
    get = cursor.fetchall()

    res = []
    arr = get[0]['cel_rashoda'].split(',')
    status = [False for i in arr]
    res.extend([arr, status])

    return jsonify(res)
@app.route('/add_cel', methods=['POST'])
def add_cel():
    request_data = request.get_json()
    user_request = {
        'businesses': request_data['businesses'],
        'cel': request_data['cel']


    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT cel_rashoda, cel_rashoda_status FROM spisok WHERE businesses = '{user_request['businesses']}';""")
    get = cursor.fetchall()


    if get[0]['cel_rashoda']:
        get[0]['cel_rashoda'] += ',' + user_request['cel']
    else:
        get[0]['cel_rashoda'] += user_request['cel']


    if get[0]['cel_rashoda_status']:
        get[0]['cel_rashoda_status'] += '/' + user_request['cel'] + '|' + 'true'
    else:
        get[0]['cel_rashoda_status'] += user_request['cel'] + '|' + 'true'








    cursor.execute(
        f"""UPDATE spisok SET cel_rashoda = '{get[0]['cel_rashoda']}',
        cel_rashoda_status = '{get[0]['cel_rashoda_status']}'
        WHERE businesses = '{user_request['businesses']}'""")
    g.db.commit()


    cursor.execute(
        f"""SELECT cel_rashoda FROM spisok WHERE businesses = '{user_request['businesses']}';""")
    get = cursor.fetchall()

    res = []
    arr = get[0]['cel_rashoda'].split(',')
    status = [False for i in arr]
    res.extend([arr, status])

    return jsonify(res)
@app.route('/rashod_position', methods=['POST'])
def rashod_position():
    request_data = request.get_json()

    user_request = {
        'businesses': request_data['businesses'],
        'cel_rashoda': request_data['cel_rashoda']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT positions_rashoda FROM spisok WHERE businesses = '{user_request['businesses']}';""")
    get = cursor.fetchall()

    if get[0]['positions_rashoda'] == '':
        return jsonify(None)
    else:

        get = get[0]['positions_rashoda'].split('/')

        arr = []

        for i in get:
            i = {i.split(':')[0]: [[j.split('-')[0], 'false'] for j in i.split(':')[1].split('|') if
                                   j.split('-')[1] != 'false']}

            for e in i:
                if e == user_request['cel_rashoda']:
                    if i[e]:
                        for k in i[e]:
                            arr.append(k[0])

        res = []
        status = [False for i in arr]
        res.extend([arr, status])
        return jsonify(res)
@app.route('/add_position_cel', methods=['POST'])
def add_position_cel():
    request_data = request.get_json()

    user_request = {
        'businesses': request_data['businesses'],
        'cel_rashoda': request_data['cel_rashoda'],
        'position': request_data['position']
    }



    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT positions_rashoda FROM spisok WHERE businesses = '{user_request['businesses']}';""")
    get = cursor.fetchall()


    trig = 0
    if get[0]['positions_rashoda']:
        get = get[0]['positions_rashoda'].split('/')
        arr = [i.split(':') for i in get]

        for i in arr:
            if i[0] == user_request['cel_rashoda']:
                i[1] += f'''|{user_request['position']}-true'''
                trig += 1


        if trig == 0:
            arr.append([user_request['cel_rashoda'], f'''{user_request['position']}-true'''])

        new = []
        for j in arr:
            j = ':'.join(j)
            new.append(j)
        add = '/'.join(new)


        cursor.execute(
            f"""UPDATE spisok SET positions_rashoda = '{add}'
                     WHERE businesses = '{user_request['businesses']}'""")
        g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT positions_rashoda FROM spisok WHERE businesses = '{user_request['businesses']}';""")
        get = cursor.fetchall()




        get = get[0]['positions_rashoda'].split('/')

        arr = []
        for i in get:
            i = {i.split(':')[0]: [[j.split('-')[0], 'false'] for j in i.split(':')[1].split('|') if
                                   j.split('-')[1] != 'false']}
            for e in i:
                if e == user_request['cel_rashoda']:
                    if i[e]:
                        for k in i[e]:
                            arr.append(k[0])

        res = []
        status = [False for i in arr]
        res.extend([arr, status])
        return jsonify(res)




    else:
        get[0]['positions_rashoda'] = f'''{user_request['cel_rashoda']}:{user_request['position']}-true'''

        cursor.execute(
            f"""UPDATE spisok SET positions_rashoda = '{get[0]['positions_rashoda']}'
              WHERE businesses = '{user_request['businesses']}'""")
        g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT positions_rashoda FROM spisok WHERE businesses = '{user_request['businesses']}';""")
        get = cursor.fetchall()

        print(get)

        get = get[0]['positions_rashoda'].split('/')

        arr = []
        for i in get:
            i = {i.split(':')[0]: [[j.split('-')[0], 'false'] for j in i.split(':')[1].split('|') if
                                   j.split('-')[1] != 'false']}
            for e in i:
                if e == user_request['cel_rashoda']:
                    if i[e]:
                        for k in i[e]:
                            arr.append(k[0])

        res = []
        status = [False for i in arr]
        res.extend([arr, status])
        return jsonify(res)
@app.route('/rashod_enter', methods=['POST'])
def rashod_enter():

    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'sum': request_data['sum'],
        'time': request_data['time'],
        'businesses': request_data['businesses'],
        'cel_rashoda': request_data['cel_rashoda'],
        'position': request_data['position']
    }


    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, money FROM users WHERE id = '{user_request['id']}';""")
    get = cursor.fetchall()
    get[0]['balance'] -= user_request['sum']
    get[0]['money'] -= user_request['sum']






    cursor.execute(
        f"""UPDATE users SET balance = '{get[0]['balance']}', money = '{get[0]['money']}' WHERE id = '{user_request['id']}';""")
    g.db.commit()

    cursor.execute(
        f"""INSERT INTO operations (user_id, summa, data, operation_type, business, istochnik, position)
        VALUES ('{user_request['id']}', '{user_request['sum']}', '{user_request['time']}', 'Расход',
        '{user_request['businesses']}', '{user_request['cel_rashoda']}', '{user_request['position']}');""")
    g.db.commit()




    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, (SELECT COUNT(perevody.perevod_status) 
        FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = '{user_request['id']}') AS perevod_status FROM users WHERE id = {user_request['id']};""")

    get = cursor.fetchall()

    return jsonify({'id': user_request['id'], 'user_name': get[0]['user_name'], 'balance': get[0]['balance'],
                    'money': get[0]['money'], 'my_money': get[0]['my_money'], 'status_count': get[0]['perevod_status'],
                    "status": get[0]['my_businesses']}, {'id': user_request['id'], 'sum': user_request['sum'],
                    'time': user_request['time'], 'businesses': user_request['businesses'],
                    'istochnik_prihoda':user_request['cel_rashoda'], 'position': user_request['position'],
                    'operation_type': 'Расход'})






@app.route('/sobstv_vnos', methods=['POST'])
def sobstv_vnos():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'sum': request_data['sum'],
        'time': request_data['time']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, my_money FROM users WHERE id = '{user_request['id']}';""")
    get = cursor.fetchall()
    get[0]['balance'] += user_request['sum']
    get[0]['my_money'] += user_request['sum']
    print(get)

    cursor.execute(
        f"""UPDATE users SET balance = '{get[0]['balance']}', my_money = '{get[0]['my_money']}' WHERE id = '{user_request['id']}';""")
    g.db.commit()

    cursor.execute(
        f"""INSERT INTO operations (user_id, summa, data, operation_type)
          VALUES ('{user_request['id']}', '{user_request['sum']}', '{user_request['time']}', 'Собственные внесено');""")
    g.db.commit()



    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, (SELECT COUNT(perevody.perevod_status) 
        FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = '{user_request['id']}') AS perevod_status FROM users WHERE id = {user_request['id']};""")

    get = cursor.fetchall()


    return jsonify({'id': user_request['id'], 'user_name': get[0]['user_name'], 'balance': get[0]['balance'],
                    'money': get[0]['money'], 'my_money': get[0]['my_money'], 'status_count': get[0]['perevod_status'],
                    "status": get[0]['my_businesses']}, {'id': user_request['id'], 'sum': user_request['sum'],
                    'time': user_request['time'], 'operation_type': 'Собственные внесено'})
@app.route('/sobstv_vivod', methods=['POST'])
def sobstv_vivod():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'sum': request_data['sum'],
        'time': request_data['time']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, my_money FROM users WHERE id = '{user_request['id']}';""")
    get = cursor.fetchall()
    get[0]['balance'] -= user_request['sum']
    get[0]['my_money'] -= user_request['sum']
    print(get)

    cursor.execute(
        f"""UPDATE users SET balance = '{get[0]['balance']}', my_money = '{get[0]['my_money']}' WHERE id = '{user_request['id']}';""")
    g.db.commit()

    cursor.execute(
        f"""INSERT INTO operations (user_id, summa, data, operation_type)
          VALUES ('{user_request['id']}', '{user_request['sum']}', '{user_request['time']}', 'Собственные выведено');""")
    g.db.commit()




    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, (SELECT COUNT(perevody.perevod_status) 
        FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = {user_request['id']}) AS perevod_status FROM users WHERE id = {user_request['id']};""")

    get = cursor.fetchall()


    return jsonify({'id': user_request['id'], 'user_name': get[0]['user_name'], 'balance': get[0]['balance'],
                    'money': get[0]['money'], 'my_money': get[0]['my_money'], 'status_count': get[0]['perevod_status'],
                    "status": get[0]['my_businesses']}, {'id': user_request['id'], 'sum': user_request['sum'],
                    'time': user_request['time'], 'operation_type': 'Собственные выведено'})







@app.route('/perevod_info', methods=['POST'])
def perevod_info():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id, user_name FROM users WHERE id != '{user_request['id']}';""")
    get = cursor.fetchall()
    print(get)

    arr = []
    new = [[i['id'], i['user_name']] for i in get]

    new2 = [False for i in get]
    arr.extend([new, new2])
    print(arr)
    return jsonify(arr)
@app.route('/perevod_enter', methods=['POST'])
def perevod_enter():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'send_id': request_data['send_id'],
        'sum': request_data['sum'],
        'data': request_data['data'],
        'send_user': request_data['send_user']

    }
    print(user_request)

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, my_money FROM users WHERE id = '{user_request['id']}';""")
    get = cursor.fetchall()
    get[0]['balance'] -= user_request['sum']
    get[0]['my_money'] -= user_request['sum']

    cursor.execute(
        f"""UPDATE users SET balance = '{get[0]['balance']}', my_money = '{get[0]['my_money']}' WHERE id = '{user_request['id']}';""")
    g.db.commit()


    cursor.execute(
        f"""INSERT INTO perevody (user_get, summa, data, user_send, perevod_status)
          VALUES ('{user_request['send_id']}', '{user_request['sum']}', '{user_request['data']}', '{user_request['id']}', 
          'false');""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, (SELECT COUNT(perevody.perevod_status) 
        FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = '{user_request['id']}') AS perevod_status FROM users WHERE id = {user_request['id']};""")

    to_page = cursor.fetchall()

    return jsonify({'id': user_request['id'], 'user_name': to_page[0]['user_name'], 'balance': to_page[0]['balance'],
                    'money': to_page[0]['money'], 'my_money': to_page[0]['my_money'], 'status_count': to_page[0]['perevod_status'],
                    "status": to_page[0]['my_businesses']}, {'businesses': 'Кому' + ' ' + user_request['send_user'], 'sum': user_request['sum'],
                                                         'time': user_request['data'],
                                                         'operation_type': 'Перевод'})
@app.route('/perevod_show', methods=['POST'])
def perevod_show():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT perevody.summa, perevody.data, users.user_name,  users.id FROM perevody INNER JOIN users ON 
        perevody.user_send = users.user_name WHERE perevody.user_get = '{user_request['id']}' AND 
        perevody.perevod_status = 'false';""")

    get = cursor.fetchall()

    res = [i | {'status': False} for i in get]



    return jsonify(res)
@app.route('/perevod_confirm', methods=['POST'])
def perevod_confirm():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id'],
        'info': request_data['info']

    }


    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, my_money FROM users WHERE id = '{user_request['id']}';""")
    get = cursor.fetchall()

    balance = get[0]['balance']
    my_money = get[0]['my_money']

    for i in user_request['info']:
        balance += i['summa']
        my_money += i['summa']

        cursor.execute(
            f"""UPDATE perevody SET perevod_status = '{str(i['status']).lower()}' WHERE user_get = '{user_request['id']}' AND
            summa = '{i['summa']}' AND data = '{i['data']}' AND user_send = '{i['send_id']}';""")
        g.db.commit()

    cursor.execute(
        f"""UPDATE users SET balance = '{balance}', my_money = '{my_money}' WHERE id = '{user_request['id']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, (SELECT COUNT(perevody.perevod_status) 
          FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = '{user_request['id']}') AS 
          perevod_status FROM users WHERE id = {user_request['id']};""")

    to_page = cursor.fetchall()

    return jsonify({'id': user_request['id'], 'user_name': to_page[0]['user_name'], 'balance': to_page[0]['balance'],
                    'money': to_page[0]['money'], 'my_money': to_page[0]['my_money'],
                    'status_count': to_page[0]['perevod_status'],
                    "status": to_page[0]['my_businesses']},
                   {'name': 'От' + ' ' + user_request['info'][-1]['user_name'], 'sum': user_request['info'][-1]['summa'],
                    'time': user_request['info'][-1]['data'],
                    'operation_type': 'Перевод'})





@app.route('/spravochniki/business', methods=['GET']) # общий для всех запросов бизнесов
def spravochniki_business():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM a_businesses;""")
    businesses = cursor.fetchall()

    return jsonify(businesses)
@app.route('/business/add', methods=['POST'])
def business_add():
    request_data = request.get_json()
    user_request = {
        'bussiness_name': request_data['bussiness_name'],
        'status': request_data['status']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""INSERT INTO a_businesses (business, status) VALUES ('{user_request['bussiness_name']}','{user_request['status']}')""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM a_businesses;""")
    businesses = cursor.fetchall()

    return jsonify(businesses)
@app.route('/bussinesses/rename', methods=['POST'])
def bussinesses_rename():
    request_data = request.get_json()
    user_request = {
        'id_business': request_data['id_business'],
        'new_name': request_data['new_name'],
        'status': request_data['status']
    }

    cursor = get_db().cursor()

    cursor.execute(
        f"""UPDATE a_businesses SET business = '{user_request['new_name']}',
                        status = '{user_request['status']}'
                        WHERE id_business = '{user_request['id_business']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM a_businesses;""")
    businesses = cursor.fetchall()

    return jsonify(businesses)
@app.route('/bussinesses/del', methods=['POST'])
def bussinesses_del():
    request_data = request.get_json()
    user_request = {
        'id_business': request_data['id_business']

    }
    ##### удаление источников/целей по бизнесу
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik_cel FROM a_keys_ist_cel WHERE id_business = '{user_request['id_business']}';""")
    check_istoch_cel = cursor.fetchall()

    for i in check_istoch_cel:
        cursor = get_db().cursor()
        cursor.execute(
            f"""DELETE FROM a_keys_ist_cel WHERE id_business = '{user_request['id_business']}' AND
             id_istochnik_cel = '{i['id_istochnik_cel']}'""")
        g.db.commit()


    for i in check_istoch_cel:
        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT COUNT(DISTINCT id_istochnik_cel) AS id_istochnik_cel FROM a_keys_ist_cel
            WHERE id_istochnik_cel = '{i['id_istochnik_cel']}'""")
        count_istoch_cel = cursor.fetchall()

        if count_istoch_cel[0]['id_istochnik_cel'] > 0:
            continue
        else:
            cursor = get_db().cursor()
            cursor.execute(
                f"""DELETE FROM a_istochniky_cely WHERE id_istochnik_cel = '{i['id_istochnik_cel']}'""")
            g.db.commit()
    ### удаление позиций по бизнесу

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id FROM a_positions_keys WHERE id_business = '{user_request['id_business']}';""")
    check_positions = cursor.fetchall()

    for i in check_positions:
        cursor = get_db().cursor()
        cursor.execute(
            f"""DELETE FROM a_positions_keys WHERE id_business = '{user_request['id_business']}' AND
                position_id = '{i['position_id']}'""")
        g.db.commit()

    for i in check_positions:
        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT COUNT(DISTINCT position_id) AS position_id FROM a_positions_keys 
            WHERE position_id = '{i['position_id']}'""")
        count_positions = cursor.fetchall()


        if count_positions[0]['position_id'] > 0:
            continue
        else:
            cursor = get_db().cursor()
            cursor.execute(
                f"""DELETE FROM a_positions WHERE position_id = '{i['position_id']}'""")
            g.db.commit()

    ### удаление самого бизнеса
    cursor = get_db().cursor()
    cursor.execute(
        f"""DELETE FROM a_businesses WHERE id_business = '{user_request['id_business']}'""")
    g.db.commit()

    ## показ бизнесов
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM a_businesses;""")
    businesses = cursor.fetchall()

    return jsonify(businesses)







@app.route('/spravochniki/istochniky', methods=['GET'])
def istochnik_prihoda():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik_cel, istochnik, a_istochniky_cely.status AS status, GROUP_CONCAT(DISTINCT business) AS business
        FROM a_businesses
        JOIN a_keys_ist_cel USING (id_business)
        JOIN a_istochniky_cely USING (id_istochnik_cel)
        WHERE istochnik IS NOT NULL
        GROUP BY istochnik
        """)
    get = cursor.fetchall()
    for i in get:
        i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/istochniky/filtr', methods=['POST'])  ### перед фильтром запос идет на spravochniki_business
def prihod_filtr():
    request_data = request.get_json()
    user_request = {
        'id_business': request_data['id_business']

    }
    r = ','.join(list(map(str,user_request['id_business'])))

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik_cel, istochnik, a_istochniky_cely.status AS status, GROUP_CONCAT(DISTINCT business) AS business
        FROM a_businesses
        JOIN a_keys_ist_cel USING (id_business)
        JOIN a_istochniky_cely USING (id_istochnik_cel)
        WHERE istochnik IS NOT NULL AND id_business IN ({r})
        GROUP BY istochnik""")
    get = cursor.fetchall()

    for i in get:
        i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/istochniky/edit', methods=['POST']) # запрос бизнесов с их статусами и привязками источников
def istoch_edit():
    request_data = request.get_json()
    user_request = {
        'istochnik_id': request_data['istochnik_id'],

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, a_businesses.status AS business_status
           FROM a_businesses
           JOIN a_keys_ist_cel USING (id_business) 
           JOIN a_istochniky_cely USING (id_istochnik_cel)
           WHERE id_istochnik_cel = '{user_request['istochnik_id']}';""")
    businesses = cursor.fetchall()

    for i in businesses:
        i.setdefault('istochnik_status', True)
    cursor.execute(
        f"""SELECT id_business, business, a_businesses.status AS business_status
           FROM a_businesses WHERE id_business NOT IN (SELECT id_business
           FROM a_businesses
           JOIN a_keys_ist_cel USING (id_business) 
           JOIN a_istochniky_cely USING (id_istochnik_cel)
           WHERE id_istochnik_cel = '{user_request['istochnik_id']}');""")

    businesses_else = cursor.fetchall()

    for i in businesses_else:
        i.setdefault('istochnik_status', None)
        businesses.append(i)

    return jsonify(businesses)
@app.route('/spravochniki/istochniky/edit_confirm', methods=['POST'])
def istoch_edit_confirm():
    request_data = request.get_json()
    user_request = {

        'id_businesses': request_data['id_businesses'],
        'id_businesses_del': request_data['id_businesses_del'],
        'istochnik_id': request_data['istochnik_id'],
        'name': request_data['name'],
        'new_name': request_data['new_name'],
        'status': request_data['status']

    }

    for i in user_request['id_businesses']:
        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO a_keys_ist_cel (id_istochnik_cel, id_business) VALUES ('{user_request['istochnik_id']}', 
            '{i}')""")
        g.db.commit()



    for i in user_request['id_businesses_del']:
        cursor = get_db().cursor()
        cursor.execute(
            f"""DELETE FROM a_keys_ist_cel WHERE id_business = '{i}' and id_istochnik_cel = '{user_request['istochnik_id']}'""")
        g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""UPDATE a_istochniky_cely SET istochnik = '{user_request['new_name']}',
            status = '{user_request['status']}'
            WHERE id_istochnik_cel = '{user_request['istochnik_id']}'""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik_cel, istochnik, a_istochniky_cely.status AS status, GROUP_CONCAT(DISTINCT business) AS business
            FROM a_businesses
            JOIN a_keys_ist_cel USING (id_business)
            JOIN a_istochniky_cely USING (id_istochnik_cel)
            WHERE istochnik IS NOT NULL
            GROUP BY istochnik
            """)
    get = cursor.fetchall()
    for i in get:
        i['business'] = i['business'].split(',')

    return jsonify(get)
# перед любым confirm идет запрос на показ  бизнесов/источников/целей c обычных запросов из справочников
@app.route('/spravochniki/istochniky/add_confirm', methods=['POST'])
def istochnik_add_confirm():
    request_data = request.get_json()
    user_request = {
        'id_business': request_data['id_business'],
        'istochnik': request_data['istochnik'],
        'status': request_data['status']

    }
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT istochnik FROM a_istochniky_cely
        WHERE istochnik = '{user_request['istochnik']}';""")
    istochniky = cursor.fetchall()

    if istochniky:
        return jsonify('Такой источник уже существует')
    else:

        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO a_istochniky_cely (istochnik, status) VALUES ('{user_request['istochnik']}', 
            '{user_request['status']}')""")
        g.db.commit()


        for i in user_request['id_business']:
            cursor.execute(
                f"""INSERT INTO a_keys_ist_cel (id_istochnik_cel, id_business) VALUES 
                ((SELECT MAX(id_istochnik_cel) FROM a_istochniky_cely), '{i}')""")
            g.db.commit()



        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT id_istochnik_cel, istochnik, a_istochniky_cely.status AS status, GROUP_CONCAT(DISTINCT business) AS business
                FROM a_businesses
                JOIN a_keys_ist_cel USING (id_business)
                JOIN a_istochniky_cely USING (id_istochnik_cel)
                WHERE istochnik IS NOT NULL
                GROUP BY istochnik
                """)
        get = cursor.fetchall()
        for i in get:
            i['business'] = i['business'].split(',')

        return jsonify(get)
@app.route('/spravochniki/istochniky/del', methods=['POST'])
def istochniky_del():
    request_data = request.get_json()
    user_request = {

        'istochnik_id': request_data['istochnik_id']

    }



    cursor = get_db().cursor()
    cursor.execute(
        f"""DELETE a_istochniky_cely, a_keys_ist_cel FROM a_istochniky_cely, a_keys_ist_cel
        WHERE a_istochniky_cely.id_istochnik_cel = {user_request['istochnik_id']}
        AND a_keys_ist_cel.id_istochnik_cel = {user_request['istochnik_id']};""")
    g.db.commit()




    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik_cel, istochnik, a_istochniky_cely.status AS status, GROUP_CONCAT(DISTINCT business) AS business
            FROM a_businesses
            JOIN a_keys_ist_cel USING (id_business)
            JOIN a_istochniky_cely USING (id_istochnik_cel)
            WHERE istochnik IS NOT NULL
            GROUP BY istochnik
            """)
    get = cursor.fetchall()
    for i in get:
        i['business'] = i['business'].split(',')

    return jsonify(get)









@app.route('/spravochniki/cely', methods=['GET'])
def cel_rashoda():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik_cel, cel, a_istochniky_cely.status AS status, GROUP_CONCAT(DISTINCT business) AS business
           FROM a_businesses
           JOIN a_keys_ist_cel USING (id_business)
           JOIN a_istochniky_cely USING (id_istochnik_cel)
           WHERE cel IS NOT NULL
           GROUP BY cel
           """)
    get = cursor.fetchall()
    for i in get:
        i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/cely/filtr', methods=['POST']) ### перед фильтром запос идет на spravochniki_business
def cel_rashoda_filtr():
    request_data = request.get_json()
    user_request = {
        'id_businesses': request_data['id_businesses']

    }
    r = ','.join(list(map(str, user_request['id_businesses'])))

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik_cel, cel, a_istochniky_cely.status AS status, GROUP_CONCAT(DISTINCT business) AS business
          FROM a_businesses
          JOIN a_keys_ist_cel USING (id_business)
          JOIN a_istochniky_cely USING (id_istochnik_cel)
          WHERE cel IS NOT NULL AND id_business IN ({r})
          GROUP BY cel""")
    get = cursor.fetchall()

    for i in get:
        i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/cel/edit', methods=['POST']) # запрос бизнесов с их статусами и привязками источников
def cel_edit():
    request_data = request.get_json()
    user_request = {
        'cel_id': request_data['cel_id']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, a_businesses.status AS business_status
              FROM a_businesses
              JOIN a_keys_ist_cel USING (id_business) 
              JOIN a_istochniky_cely USING (id_istochnik_cel)
              WHERE id_istochnik_cel = '{user_request['cel_id']}';""")
    businesses = cursor.fetchall()

    for i in businesses:
        i.setdefault('cel_status', True)
    cursor.execute(
        f"""SELECT id_business, business, a_businesses.status AS business_status
              FROM a_businesses WHERE id_business NOT IN (SELECT id_business
              FROM a_businesses
              JOIN a_keys_ist_cel USING (id_business) 
              JOIN a_istochniky_cely USING (id_istochnik_cel)
              WHERE id_istochnik_cel = '{user_request['cel_id']}');""")

    businesses_else = cursor.fetchall()

    for i in businesses_else:
        i.setdefault('cel_status', None)
        businesses.append(i)

    return jsonify(businesses)
@app.route('/spravochniki/cel/edit_confirm', methods=['POST'])
def cel_edit_confirm():
    request_data = request.get_json()
    user_request = {

        'id_businesses': request_data['id_businesses'],
        'id_businesses_del': request_data['id_businesses_del'],
        'cel_id': request_data['cel_id'],
        'name': request_data['name'],
        'new_name': request_data['new_name'],
        'status': request_data['status']

    }

    for i in user_request['id_businesses']:
        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO a_keys_ist_cel (id_istochnik_cel, id_business) VALUES ('{user_request['cel_id']}', 
                '{i}')""")
        g.db.commit()

    for i in user_request['id_businesses_del']:
        cursor = get_db().cursor()
        cursor.execute(
            f"""DELETE FROM a_keys_ist_cel WHERE id_business = '{i}' and id_istochnik_cel = '{user_request['cel_id']}'""")
        g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""UPDATE a_istochniky_cely SET cel = '{user_request['new_name']}',
                status = '{user_request['status']}'
                WHERE id_istochnik_cel = '{user_request['cel_id']}'""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik_cel, cel, a_istochniky_cely.status AS status, GROUP_CONCAT(DISTINCT business) AS business
                FROM a_businesses
                JOIN a_keys_ist_cel USING (id_business)
                JOIN a_istochniky_cely USING (id_istochnik_cel)
                WHERE cel IS NOT NULL
                GROUP BY cel
                """)
    get = cursor.fetchall()
    for i in get:
        i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/cel/add_confirm', methods=['POST'])
def cel_add_confirm():
    request_data = request.get_json()
    user_request = {
        'id_business': request_data['id_business'],
        'cel': request_data['cel'],
        'status': request_data['status']

    }
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT cel FROM a_istochniky_cely
           WHERE cel = '{user_request['cel']}';""")
    cel = cursor.fetchall()

    if cel:
        return jsonify('Такая цель уже существует')
    else:

        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO a_istochniky_cely (cel, status) VALUES ('{user_request['cel']}', 
               '{user_request['status']}')""")
        g.db.commit()

        for i in user_request['id_business']:
            cursor.execute(
                f"""INSERT INTO a_keys_ist_cel (id_istochnik_cel, id_business) VALUES 
                   ((SELECT MAX(id_istochnik_cel) FROM a_istochniky_cely), '{i}')""")
            g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT id_istochnik_cel, cel, a_istochniky_cely.status AS status, GROUP_CONCAT(DISTINCT business) AS business
                   FROM a_businesses
                   JOIN a_keys_ist_cel USING (id_business)
                   JOIN a_istochniky_cely USING (id_istochnik_cel)
                   WHERE cel IS NOT NULL
                   GROUP BY cel
                   """)
        get = cursor.fetchall()
        for i in get:
            i['business'] = i['business'].split(',')

        return jsonify(get)
@app.route('/spravochniki/cel/del', methods=['POST'])
def cel_del():
    request_data = request.get_json()
    user_request = {

        'cel_id': request_data['cel_id']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""DELETE a_istochniky_cely, a_keys_ist_cel FROM a_istochniky_cely, a_keys_ist_cel
           WHERE a_istochniky_cely.id_istochnik_cel = {user_request['cel_id']}
           AND a_keys_ist_cel.id_istochnik_cel = {user_request['cel_id']};""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik_cel, cel, a_istochniky_cely.status AS status, GROUP_CONCAT(DISTINCT business) AS business
                       FROM a_businesses
                       JOIN a_keys_ist_cel USING (id_business)
                       JOIN a_istochniky_cely USING (id_istochnik_cel)
                       WHERE cel IS NOT NULL
                       GROUP BY cel
                       """)
    get = cursor.fetchall()
    for i in get:
        i['business'] = i['business'].split(',')

    return jsonify(get)

@app.route('/spravochniki/positions', methods=['GET'])
def positions():

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id, position, a_positions.status, GROUP_CONCAT(DISTINCT istochnik) AS istochnik, 
            GROUP_CONCAT(DISTINCT cel) as cel, GROUP_CONCAT(DISTINCT business) AS business
            FROM a_businesses
            JOIN a_keys_ist_cel USING(id_business)
            JOIN a_istochniky_cely USING(id_istochnik_cel)
            JOIN a_positions_keys USING(id_istochnik_cel, id_business)
            JOIN a_positions USING(position_id)

            GROUP BY position""")

    get = cursor.fetchall()

    for i in get:
        if i['cel'] != None:
            i['cel'] = i['cel'].split(',')
        if i['istochnik'] != None:
            i['istochnik'] = i['istochnik'].split(',')
        if i['business'] != None:
            i['business'] = i['business'] .split(',')


    return jsonify(get)
@app.route('/spravochniki/positions/filtr/business', methods=['POST'])
def positions_business():
    request_data = request.get_json()
    user_request = {

        'id_businesses': request_data['id_businesses'],


    }
    r = ','.join(list(map(str, user_request['id_businesses'])))

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id, position, a_positions.status, GROUP_CONCAT(DISTINCT istochnik) AS istochnik,
        GROUP_CONCAT(DISTINCT cel) as cel, GROUP_CONCAT(DISTINCT business) AS business
        FROM a_businesses
        JOIN a_keys_ist_cel USING(id_business)
        JOIN a_istochniky_cely USING(id_istochnik_cel)
        JOIN a_positions_keys USING(id_istochnik_cel, id_business)
        JOIN a_positions USING(position_id)
        WHERE id_business IN ({r})
        GROUP BY position""")


    get = cursor.fetchall()
    for i in get:
        if i['cel'] != None:
            i['cel'] = i['cel'].split(',')
        if i['istochnik'] != None:
            i['istochnik'] = i['istochnik'].split(',')
        if i['business'] != None:
            i['business'] = i['business'] .split(',')

    return jsonify(get)
@app.route('/spravochniki/positions/filtr/istochniky', methods=['POST'])
def positions_istochniky():
    request_data = request.get_json()
    user_request = {

        'id_istochniki': request_data['id_istochniki']

    }
    r = ','.join(list(map(str, user_request['id_istochniki'])))

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id, position, a_positions.status, GROUP_CONCAT(DISTINCT istochnik) AS istochnik,
           GROUP_CONCAT(DISTINCT business) AS business
           FROM a_businesses
           JOIN a_keys_ist_cel USING(id_business)
           JOIN a_istochniky_cely USING(id_istochnik_cel)
           JOIN a_positions_keys USING(id_istochnik_cel, id_business)
           JOIN a_positions USING(position_id)
           WHERE id_istochnik_cel IN ({r})
           GROUP BY position""")

    get = cursor.fetchall()
    for i in get:
        if i['istochnik'] != None:
            i['istochnik'] = i['istochnik'].split(',')
        if i['business'] != None:
            i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/positions/filtr/cel', methods=['POST'])
def positions_cel():
    request_data = request.get_json()
    user_request = {

        'id_cel': request_data['id_cel']

    }
    r = ','.join(list(map(str, user_request['id_cel'])))

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id, position, a_positions.status, GROUP_CONCAT(DISTINCT cel) AS cel,
               GROUP_CONCAT(DISTINCT business) AS business
               FROM a_businesses
               JOIN a_keys_ist_cel USING(id_business)
               JOIN a_istochniky_cely USING(id_istochnik_cel)
               JOIN a_positions_keys USING(id_istochnik_cel, id_business)
               JOIN a_positions USING(position_id)
               WHERE id_istochnik_cel IN ({r})
               GROUP BY position""")

    get = cursor.fetchall()
    for i in get:
        if i['cel'] != None:
            i['cel'] = i['cel'].split(',')
        if i['business'] != None:
            i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/positions/filtr/positions_buss_istoch_cel', methods=['POST'])
def positions_buss_istoch_cel():
    request_data = request.get_json()
    user_request = {

        'id_businesses': request_data['id_businesses'],
        'id_ist_cel': request_data['id_ist_cel']

    }
    id_businesses = ','.join(list(map(str, user_request['id_businesses'])))
    id_ist_cel = ','.join(list(map(str, user_request['id_ist_cel'])))
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id, position, a_positions.status, GROUP_CONCAT(DISTINCT istochnik) AS istochnik,
                GROUP_CONCAT(DISTINCT cel) as cel, GROUP_CONCAT(DISTINCT business) AS business
                FROM a_businesses
                JOIN a_keys_ist_cel USING(id_business)
                JOIN a_istochniky_cely USING(id_istochnik_cel)
                JOIN a_positions_keys USING(id_istochnik_cel, id_business)
                JOIN a_positions USING(position_id)
                WHERE id_business IN ({id_businesses}) AND id_istochnik_cel IN ({id_ist_cel})
                GROUP BY position""")

    get = cursor.fetchall()

    for i in get:
        if i['cel'] != None:
            i['cel'] = i['cel'].split(',')
        if i['istochnik'] != None:
            i['istochnik'] = i['istochnik'].split(',')
        if i['business'] != None:
            i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/positions/filtr/positions_buss_istochnik', methods=['POST'])
def positions_buss_istoch():
    request_data = request.get_json()
    user_request = {

        'id_businesses': request_data['id_businesses'],
        'id_ist_cel': request_data['id_ist_cel']

    }
    id_businesses = ','.join(list(map(str, user_request['id_businesses'])))
    id_ist_cel = ','.join(list(map(str, user_request['id_ist_cel'])))
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id, position, a_positions.status, GROUP_CONCAT(DISTINCT istochnik) AS istochnik,
                GROUP_CONCAT(DISTINCT business) AS business
                FROM a_businesses
                JOIN a_keys_ist_cel USING(id_business)
                JOIN a_istochniky_cely USING(id_istochnik_cel)
                JOIN a_positions_keys USING(id_istochnik_cel, id_business)
                JOIN a_positions USING(position_id)
                WHERE id_business IN ({id_businesses}) AND id_istochnik_cel IN ({id_ist_cel})
                GROUP BY position""")

    get = cursor.fetchall()

    for i in get:
        if i['istochnik'] != None:
            i['istochnik'] = i['istochnik'].split(',')
        if i['business'] != None:
            i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/positions/filtr/positions_buss_cel', methods=['POST'])
def positions_buss_cel():
    request_data = request.get_json()
    user_request = {

        'id_businesses': request_data['id_businesses'],
        'id_ist_cel': request_data['id_ist_cel']

    }
    id_businesses = ','.join(list(map(str, user_request['id_businesses'])))
    id_ist_cel = ','.join(list(map(str, user_request['id_ist_cel'])))
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id, position, a_positions.status, 
                   GROUP_CONCAT(DISTINCT cel) as cel, GROUP_CONCAT(DISTINCT business) AS business
                   FROM a_businesses
                   JOIN a_keys_ist_cel USING(id_business)
                   JOIN a_istochniky_cely USING(id_istochnik_cel)
                   JOIN a_positions_keys USING(id_istochnik_cel, id_business)
                   JOIN a_positions USING(position_id)
                   WHERE id_business IN ({id_businesses}) AND id_istochnik_cel IN ({id_ist_cel})
                   GROUP BY position""")

    get = cursor.fetchall()

    for i in get:
        if i['cel'] != None:
            i['cel'] = i['cel'].split(',')
        if i['business'] != None:
            i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/positions/filtr/positions_istoch_cel', methods=['POST'])
def positions_istoch_cel():
    request_data = request.get_json()
    user_request = {

        'id_ist_cel': request_data['id_ist_cel']

    }
    id_ist_cel = ','.join(list(map(str, user_request['id_ist_cel'])))

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id, position, a_positions.status, GROUP_CONCAT(DISTINCT istochnik) AS istochnik,
                GROUP_CONCAT(DISTINCT cel) as cel, GROUP_CONCAT(DISTINCT business) AS business
                FROM a_businesses
                JOIN a_keys_ist_cel USING(id_business)
                JOIN a_istochniky_cely USING(id_istochnik_cel)
                JOIN a_positions_keys USING(id_istochnik_cel, id_business)
                JOIN a_positions USING(position_id)
                WHERE id_istochnik_cel IN ({id_ist_cel})
                GROUP BY position""")

    get = cursor.fetchall()

    for i in get:
        if i['cel'] != None:
            i['cel'] = i['cel'].split(',')
        if i['istochnik'] != None:
            i['istochnik'] = i['istochnik'].split(',')
        if i['business'] != None:
            i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/position/edit/business', methods=['POST'])
def position_edit_business():
    request_data = request.get_json()
    user_request = {
        'position_id': request_data['position_id']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT DISTINCT id_business, business, a_businesses.status AS business_status
        FROM a_businesses
        JOIN a_keys_ist_cel USING(id_business)
        JOIN a_istochniky_cely USING(id_istochnik_cel)
        JOIN a_positions_keys USING(id_istochnik_cel, id_business)
        JOIN a_positions USING(position_id)
        WHERE position_id = '{user_request['position_id']}';""")
    businesses = cursor.fetchall()

    for i in businesses:
        i.setdefault('posit_status', True)
    cursor.execute(
        f"""SELECT id_business, business, a_businesses.status AS business_status
        FROM a_businesses
        WHERE id_business NOT IN (SELECT DISTINCT id_business
        FROM a_businesses
        JOIN a_keys_ist_cel USING(id_business)
        JOIN a_istochniky_cely USING(id_istochnik_cel)
        JOIN a_positions_keys USING(id_istochnik_cel, id_business)
        JOIN a_positions USING(position_id)
        WHERE position_id = '{user_request['position_id']}')""")

    businesses_else = cursor.fetchall()
    for i in businesses_else:
        i.setdefault('posit_status', None)
        businesses.append(i)


    return jsonify(businesses)
@app.route('/spravochniki/position/edit/istoch_cel', methods=['POST'])
def position_edit_istoch_cel():
    request_data = request.get_json()
    user_request = {
        'position_id': request_data['position_id'],
        'id_business': request_data['id_business']

    }


    istoch = [] #### окончательные списки куда помещаются бизнеса в виде ключей и остальное в виде значений
    cely = []

    for i in user_request['id_business']:
        istoch_spisok = [] ##### подсписки в которые помещаются отфильтрованные в sql запросах источники и цели
        cely_spisok = []
        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT DISTINCT business, id_istochnik_cel, istochnik, a_istochniky_cely.status AS status
                FROM a_businesses
                JOIN a_keys_ist_cel USING(id_business)
                JOIN a_istochniky_cely USING(id_istochnik_cel)
                JOIN a_positions_keys USING(id_istochnik_cel, id_business)
                JOIN a_positions USING(position_id)
                WHERE position_id = '{user_request['position_id']}' AND id_business = {i}
                AND istochnik IS NOT NULL
                """)
        istoch_filtred = cursor.fetchall()
        for e in istoch_filtred:
            e.setdefault('posit_status', True)

        cursor.execute(
            f"""SELECT DISTINCT business, id_istochnik_cel, istochnik, a_istochniky_cely.status AS status
                FROM a_businesses
                JOIN a_keys_ist_cel USING(id_business)
                JOIN a_istochniky_cely USING(id_istochnik_cel) WHERE id_business = {i} AND 
                istochnik IS NOT NULL AND
                id_istochnik_cel NOT IN 
                (SELECT DISTINCT id_istochnik_cel
                FROM a_businesses
                JOIN a_keys_ist_cel USING(id_business)
                JOIN a_istochniky_cely USING(id_istochnik_cel)
                JOIN a_positions_keys USING(id_istochnik_cel, id_business)
                JOIN a_positions USING(position_id)
                WHERE position_id = '{user_request['position_id']}' AND id_business = {i}
                AND istochnik IS NOT NULL) """)
        istoch_else = cursor.fetchall()
        for e in istoch_else:
            e.setdefault('posit_status', None)

        if istoch_filtred:
            for e in istoch_filtred:
                istoch_spisok.append(e)
        if istoch_else:
            for e in istoch_else:
                istoch_spisok.append(e)
        istoch.append({istoch_spisok[0]['business']: istoch_spisok})

        #########################################
        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT DISTINCT business, id_istochnik_cel, cel, a_istochniky_cely.status AS status
                        FROM a_businesses
                        JOIN a_keys_ist_cel USING(id_business)
                        JOIN a_istochniky_cely USING(id_istochnik_cel)
                        JOIN a_positions_keys USING(id_istochnik_cel, id_business)
                        JOIN a_positions USING(position_id)
                        WHERE position_id = '{user_request['position_id']}' AND id_business = {i}
                        AND cel IS NOT NULL
                        """)
        cel_filtred = cursor.fetchall()
        for e in cel_filtred:
            e.setdefault('posit_status', True)

        cursor.execute(
            f"""SELECT DISTINCT business, id_istochnik_cel, cel, a_istochniky_cely.status AS status
                        FROM a_businesses
                        JOIN a_keys_ist_cel USING(id_business)
                        JOIN a_istochniky_cely USING(id_istochnik_cel) WHERE id_business = {i} AND 
                        cel IS NOT NULL AND
                        id_istochnik_cel NOT IN 
                        (SELECT DISTINCT id_istochnik_cel
                        FROM a_businesses
                        JOIN a_keys_ist_cel USING(id_business)
                        JOIN a_istochniky_cely USING(id_istochnik_cel)
                        JOIN a_positions_keys USING(id_istochnik_cel, id_business)
                        JOIN a_positions USING(position_id)
                        WHERE position_id = '{user_request['position_id']}' AND id_business = {i}
                        AND cel IS NOT NULL) """)
        cel_else = cursor.fetchall()
        for e in cel_else:
            e.setdefault('posit_status', None)

        if cel_filtred: #### проверка на приход с sql запроса
            for e in cel_filtred:
                cely_spisok.append(e)
        if cel_else:
            for e in cel_else:
                cely_spisok.append(e)
        cely.append({cely_spisok[0]['business']: cely_spisok})



    for i in istoch:
        for e in i:
            for g in i[e]:
                del g['business']

    for i in cely:
        for e in i:
            for g in i[e]:
                del g['business']


    return jsonify({'источники': istoch, 'цели': cely})
@app.route('/spravochniki/position/edit_confirm', methods=['POST'])
def position_edit_confirm():

    request_data = request.get_json()
    user_request = {

        'ids': request_data['ids'],
        'ids_del': request_data['ids_del'],
        'position_id': request_data['position_id'],
        'name': request_data['name'],
        'new_name': request_data['new_name'],
        'status': request_data['status']

    }

    for i in user_request['ids']:
        for e in user_request['ids'][i]:
            cursor = get_db().cursor()
            cursor.execute(
                f"""INSERT INTO a_positions_keys (position_id, id_istochnik_cel, id_business) VALUES ('{user_request['position_id']}',
                    '{e}', '{i}')""")
            g.db.commit()

    for i in user_request['ids_del']:
        for e in user_request['ids_del'][i]:
            cursor = get_db().cursor()
            cursor.execute(
                f"""DELETE FROM a_positions_keys WHERE id_istochnik_cel = '{e}' AND position_id = '{user_request['position_id']}'
                AND id_business = '{i}' """)
            g.db.commit()



    cursor = get_db().cursor()
    cursor.execute(
        f"""UPDATE a_positions SET position = '{user_request['new_name']}',
                status = '{user_request['status']}'
                WHERE position_id = '{user_request['position_id']}'""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id, position, a_positions.status, GROUP_CONCAT(DISTINCT istochnik) AS istochnik, 
               GROUP_CONCAT(DISTINCT cel) as cel, GROUP_CONCAT(DISTINCT business) AS business
               FROM a_businesses
               JOIN a_keys_ist_cel USING(id_business)
               JOIN a_istochniky_cely USING(id_istochnik_cel)
               JOIN a_positions_keys USING(id_istochnik_cel, id_business)
               JOIN a_positions USING(position_id)

               GROUP BY position""")

    get = cursor.fetchall()

    for i in get:
        if i['cel'] != None:
            i['cel'] = i['cel'].split(',')
        if i['istochnik'] != None:
            i['istochnik'] = i['istochnik'].split(',')
        if i['business'] != None:
            i['business'] = i['business'].split(',')

    return jsonify(get)
@app.route('/spravochniki/position/add_confirm', methods=['POST'])
def position_add_confirm():
    request_data = request.get_json()
    user_request = {
        'ids': request_data['ids'],
        'position': request_data['position'],
        'status': request_data['status']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position FROM a_positions
           WHERE position = '{user_request['position']}';""")
    position = cursor.fetchall()

    if position:
        return jsonify('Такая позиция уже существует')
    else:
        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO a_positions (position, status) VALUES ('{user_request['position']}', 
               '{user_request['status']}')""")
        g.db.commit()

        for i in user_request['ids']:
            for e in user_request['ids'][i]:
                cursor.execute(
                    f"""INSERT INTO a_positions_keys (position_id, id_istochnik_cel, id_business) VALUES 
                       ((SELECT MAX(position_id) FROM a_positions), '{e}', '{i}')""")
                g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT position_id, position, a_positions.status, GROUP_CONCAT(DISTINCT istochnik) AS istochnik, 
                   GROUP_CONCAT(DISTINCT cel) as cel, GROUP_CONCAT(DISTINCT business) AS business
                   FROM a_businesses
                   JOIN a_keys_ist_cel USING(id_business)
                   JOIN a_istochniky_cely USING(id_istochnik_cel)
                   JOIN a_positions_keys USING(id_istochnik_cel, id_business)
                   JOIN a_positions USING(position_id)

                   GROUP BY position""")

        get = cursor.fetchall()

        for i in get:
            if i['cel'] != None:
                i['cel'] = i['cel'].split(',')
            if i['istochnik'] != None:
                i['istochnik'] = i['istochnik'].split(',')
            if i['business'] != None:
                i['business'] = i['business'].split(',')

        return jsonify(get)
@app.route('/spravochniki/position/del', methods=['POST'])
def position_del():
    request_data = request.get_json()
    user_request = {

        'position_id': request_data['position_id']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""DELETE a_positions, a_positions_keys FROM a_positions, a_positions_keys
           WHERE a_positions.position_id = {user_request['position_id']}
           AND a_positions_keys.position_id = {user_request['position_id']};""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position_id, position, a_positions.status, GROUP_CONCAT(DISTINCT istochnik) AS istochnik, 
                       GROUP_CONCAT(DISTINCT cel) as cel, GROUP_CONCAT(DISTINCT business) AS business
                       FROM a_businesses
                       JOIN a_keys_ist_cel USING(id_business)
                       JOIN a_istochniky_cely USING(id_istochnik_cel)
                       JOIN a_positions_keys USING(id_istochnik_cel, id_business)
                       JOIN a_positions USING(position_id)

                       GROUP BY position""")

    get = cursor.fetchall()

    for i in get:
        if i['cel'] != None:
            i['cel'] = i['cel'].split(',')
        if i['istochnik'] != None:
            i['istochnik'] = i['istochnik'].split(',')
        if i['business'] != None:
            i['business'] = i['business'].split(',')

    return jsonify(get)


@app.route('/spravochniki/users', methods=['GET'])
def spravochniki_users():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id, user_name, admin_status, phone_numb, email, status FROM users;""")
    users = cursor.fetchall()

    return jsonify(users)
@app.route('/spravochniki/users/edit_confirm', methods=['POST']) # все данные уже есть при предыдущем запросе
def users_edit_confirm():
    request_data = request.get_json()
    user_request = {

        'id': request_data['id'],
        'user_name': request_data['user_name'],
        'admin_status': request_data['admin_status'],
        'phone_numb': request_data['phone_numb'],
        'email': request_data['email'],
        'status': request_data['status']

    }

    cursor = get_db().cursor()

    cursor.execute(
        f"""UPDATE users SET user_name = '{user_request['user_name']}',
                           admin_status = '{user_request['admin_status']}',
                           phone_numb = '{user_request['phone_numb']}',
                           email = '{user_request['email']}',
                           status = '{user_request['status']}'                  
                           WHERE id = '{user_request['id']}'""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id, user_name, admin_status, phone_numb, email, status FROM users;""")
    users = cursor.fetchall()

    return jsonify(users)
@app.route('/spravochniki/users/user_add', methods=['POST'])
def users_add():
    request_data = request.get_json()
    user_request = {

        'user_name': request_data['user_name'],
        'admin_status': request_data['admin_status'],
        'phone_numb': request_data['phone_numb'],
        'email': request_data['email'],
        'status': request_data['status'],
        'loggin': request_data['loggin'],
        'password': request_data['password'],

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_name FROM users WHERE user_name = '{user_request['user_name']}';""")
    users = cursor.fetchall()

    if users:
        return jsonify('Такой пользователь уже существует')
    else:

        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO users (user_name, admin_status, phone_numb, email, status, loggin, password) 
            VALUES ('{user_request['user_name']}', 
            '{user_request['admin_status']}', '{user_request['phone_numb']}',
            '{user_request['email']}', '{user_request['status']}', '{user_request['loggin']}', '{user_request['password']}')""")
        g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT id, user_name, admin_status, phone_numb, email, status FROM users;""")
        users = cursor.fetchall()

        return jsonify(users)
@app.route('/spravochniki/users/user_dell', methods=['POST'])
def users_dell():
    request_data = request.get_json()
    user_request = {
        'user_id': request_data['user_id']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""DELETE FROM users WHERE id = '{user_request['user_id']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id, user_name, admin_status, phone_numb, email, status FROM users;""")
    users = cursor.fetchall()

    return jsonify(users)











@app.route('/otchety_balacne', methods=['POST'])
def otchety_balacne():

    request_data = request.get_json()
    user_request = {

        'id': request_data['id'],
        'date': request_data['date']

    }


    prihod = 0
    rashod = 0



    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.id, user_name, summa, data a FROM perevody INNER JOIN users ON perevody.user_send = users.id
        WHERE users.id = '{user_request['id']}'
        AND data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")
    perevod_to_someone = cursor.fetchall()


    cursor.execute(
        f"""SELECT users.id, user_name, summa, data FROM perevody INNER JOIN users ON perevody.user_get = users.id
        WHERE users.id = '{user_request['id']}'
        AND data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")
    perevod_to_me = cursor.fetchall()

    if perevod_to_someone:
        for i in perevod_to_someone:
            rashod += i['summa']

    if perevod_to_me:
        for i in perevod_to_me:
            prihod += i['summa']



    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.user_name, operations.summa, operations.data, operations.operation_type,
            operations.business, operations.istochnik,
            operations.position FROM operations INNER JOIN users ON operations.user_id = users.id
            WHERE users.id = '{user_request['id']}'
            AND data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")

    operations = cursor.fetchall()

    if operations:
        for i in operations:
            if i['operation_type'] == 'Приход' or i['operation_type'] == 'Собственные внос':
                prihod += i['summa']

            if i['operation_type'] == 'Расход' or i['operation_type'] == 'Собственные вывод':
                rashod += i['summa']

    cursor.execute(
        f"""SELECT users.user_name, operations.summa, operations.data, operations.operation_type,
               operations.business, operations.istochnik,
               operations.position FROM operations INNER JOIN users ON operations.user_id = users.id
               WHERE data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")

    itog = prihod - rashod


    operations_all = cursor.fetchall()

    return jsonify({'user': {'itog': itog, 'prihod': prihod, 'rashod': rashod,
                             'perevod_to_someone': perevod_to_someone,
                             'perevod_to_me': perevod_to_me, 'user_operations': operations}, 'date_operations': operations_all})
@app.route('/otchety_business', methods=['POST'])
def otchety_business():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'date': request_data['date'],
        'business': request_data['business']

    }
    daterange = pd.date_range(user_request['date'][0], user_request['date'][1])
    prihod = 0
    rashod = 0

    arr = []
    arr2 = []
    for i in daterange:
        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT user_get, user_send, summa, data FROM perevody WHERE user_get = '{user_request['id']}'
                     AND data REGEXP '{i.strftime("%d-%m-%Y")}' OR user_send = '{user_request['id']}'
                     AND data REGEXP '{i.strftime("%d-%m-%Y")}';""")
        perevody = cursor.fetchall()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT users.user_name, operations.summa, operations.data, operations.operation_type,
                 operations.business, operations.istochnik,
                 operations.position FROM operations INNER JOIN users ON operations.user_id = users.id WHERE
                 users.id = '{user_request['id']}' AND data REGEXP '{i.strftime("%d-%m-%Y")}' OR users.id = '{user_request['id']}' AND
                 data REGEXP '{i.strftime("%d-%m-%Y")}';""")

        operations = cursor.fetchall()
        if perevody:
            for e in perevody:
                arr2.append(e)

        if operations:
            for e in operations:
                arr.append(e)
                if e['operation_type'] == 'расход' and e['business'] == user_request['business']:
                    rashod += e['summa']
                elif e['operation_type'] == 'приход' and e['business'] == user_request['business']:
                    prihod += e['summa']

    itog = prihod - rashod

    return jsonify({'итог': itog, 'приход': prihod, 'расход': rashod, 'операции': arr, 'переводы': arr2})
@app.route('/otchety_users', methods=['POST'])
def otchety_users():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'date': request_data['date'],
        'req_user': request_data['req_user']

    }

    daterange = pd.date_range(user_request['date'][0], user_request['date'][1])

    spisok_perevody = []
    spisok_operations = []
    for i in daterange:
        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT user_get, user_send, summa, data FROM perevody WHERE user_get = '{user_request['req_user']}'
            AND data REGEXP '{i.strftime("%d-%m-%Y")}' OR user_send = '{user_request['req_user']}'
            AND data REGEXP '{i.strftime("%d-%m-%Y")}'
            OR
            user_get = '{user_request['id']}'
            AND data REGEXP '{i.strftime("%d-%m-%Y")}' OR user_send = '{user_request['id']}'
            AND data REGEXP '{i.strftime("%d-%m-%Y")}'
            ;""")
        get = cursor.fetchall()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT users.user_name, operations.user_id, operations.summa, operations.data, operations.operation_type,
                       operations.business, operations.istochnik,
                       operations.position FROM operations INNER JOIN users ON operations.user_id = users.id WHERE
                       users.id = '{user_request['id']}' AND data REGEXP '{i.strftime("%d-%m-%Y")}' OR
                       users.id = '{user_request['req_user']}' AND data REGEXP '{i.strftime("%d-%m-%Y")}'
                       ;""")

        get2 = cursor.fetchall()
        if get2:
            for e in get2:
                spisok_operations.append(e)
        if get:
            for e in get:
                spisok_perevody.append(e)

    poluch = 0
    otprav = 0
    for i in spisok_perevody:
        if str(i['user_get']) == user_request['req_user']:
            poluch += i['summa']
        elif str(i['user_send']) == user_request['req_user']:
            otprav += i['summa']

    vneseno = 0
    vivideno = 0

    prihod = 0
    rashod = 0

    for i in spisok_operations:
        if i['operation_type'] == 'Собственные внесено' and str(i['user_id']) == user_request['req_user']:
            vneseno += i['summa']
        elif i['operation_type'] == 'Собственные выведено' and str(i['user_id']) == user_request['req_user']:
            vivideno += i['summa']

        elif i['operation_type'] == 'приход' and str(i['user_id']) == user_request['req_user']:
            prihod += i['summa']
        elif i['operation_type'] == 'расход' and str(i['user_id']) == user_request['req_user']:
            rashod += i['summa']

    spisok_perevody = [i for i in spisok_perevody if str(i['user_get']) == user_request['id'] or
                       str(i['user_send']) == user_request['id']]
    spisok_operations = [i for i in spisok_operations if str(i['user_id']) == user_request['id']]

    return jsonify({'получено': poluch, 'отправлено': otprav, 'внесено': vneseno, 'выведено': vivideno,
                    'приход': prihod, 'расход': rashod, 'список переводов': spisok_perevody,
                    'список операций': spisok_operations})
@app.route('/otchety_istochnik', methods=['POST'])
def otchety_istochnik():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'date': request_data['date'],
        'istochnik': request_data['istochnik'],
        'business': request_data['business']

    }

    daterange = pd.date_range(user_request['date'][0], user_request['date'][1])
    perevody_spisok = []
    opertations_spisok = []

    prihod = 0
    buss_prihod = 0
    for i in daterange:
        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT user_get, user_send, summa, data FROM perevody WHERE user_get = '{user_request['id']}'
                             AND data REGEXP '{i.strftime("%d-%m-%Y")}' OR user_send = '{user_request['id']}'
                             AND data REGEXP '{i.strftime("%d-%m-%Y")}';""")
        perevody = cursor.fetchall()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT users.user_name, operations.summa, operations.data, operations.operation_type,
                         operations.business, operations.istochnik,
                         operations.position FROM operations INNER JOIN users ON operations.user_id = users.id WHERE
                         users.id = '{user_request['id']}' AND data REGEXP '{i.strftime("%d-%m-%Y")}' OR users.id = '{user_request['id']}' AND
                         data REGEXP '{i.strftime("%d-%m-%Y")}';""")

        operations = cursor.fetchall()

        if perevody:
            for e in perevody:
                perevody_spisok.append(e)

        if user_request['business'] == 'все':
            for e in operations:
                opertations_spisok.append(e)
                if e['operation_type'] == 'приход':
                    prihod += e['summa']
                if e['istochnik'] == user_request['istochnik']:
                    buss_prihod += e['summa']
        else:
            for e in operations:
                opertations_spisok.append(e)
                if e['operation_type'] == 'приход' and e['business'] == user_request['business']:
                    prihod += e['summa']
                if e['istochnik'] == user_request['istochnik'] and e['business'] == user_request['business']:
                    buss_prihod += e['summa']

    ostatok = round(buss_prihod / prihod, 3) * 100

    return jsonify({'приход за источник': buss_prihod, 'остаток': ostatok, 'список переводов': perevody_spisok,
                    'список операций': opertations_spisok})
@app.route('/otchety_cel', methods=['POST'])
def otchety_cel():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'date': request_data['date'],
        'cel': request_data['cel'],
        'business': request_data['business']

    }

    daterange = pd.date_range(user_request['date'][0], user_request['date'][1])
    perevody_spisok = []
    opertations_spisok = []

    rashod = 0
    buss_rashod = 0
    for i in daterange:
        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT user_get, user_send, summa, data FROM perevody WHERE user_get = '{user_request['id']}'
                             AND data REGEXP '{i.strftime("%d-%m-%Y")}' OR user_send = '{user_request['id']}'
                             AND data REGEXP '{i.strftime("%d-%m-%Y")}';""")
        perevody = cursor.fetchall()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT users.user_name, operations.summa, operations.data, operations.operation_type,
                         operations.business, operations.istochnik,
                         operations.position FROM operations INNER JOIN users ON operations.user_id = users.id WHERE
                         users.id = '{user_request['id']}' AND data REGEXP '{i.strftime("%d-%m-%Y")}' OR users.id = '{user_request['id']}' AND
                         data REGEXP '{i.strftime("%d-%m-%Y")}';""")

        operations = cursor.fetchall()

        if perevody:
            for e in perevody:
                perevody_spisok.append(e)

        if user_request['business'] == 'все':
            for e in operations:
                opertations_spisok.append(e)
                if e['operation_type'] == 'расход':
                    rashod += e['summa']
                if e['operation_type'] == 'расход' and e['istochnik'] == user_request['cel']:
                    buss_rashod += e['summa']
        else:
            for e in operations:
                opertations_spisok.append(e)
                if e['operation_type'] == 'расход' and e['business'] == user_request['business']:
                    rashod += e['summa']
                if e['istochnik'] == user_request['cel'] and e['business'] == user_request['business']:
                    buss_rashod += e['summa']

    ostatok = round((buss_rashod / rashod) * 100, 3)
    print(rashod, buss_rashod)
    print(ostatok)
    return jsonify({'расход за цель': buss_rashod, 'остаток': ostatok, 'список переводов': perevody_spisok,
                    'список операций': opertations_spisok})












if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5021)


