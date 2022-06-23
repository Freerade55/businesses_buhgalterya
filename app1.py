
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









@app.route('/autorization', methods=['POST']) #
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
        if user_request['loggin'] == i['loggin'] and user_request['password'] == i['password']:
            cursor = get_db().cursor()
            cursor.execute(
                f"""SELECT COUNT(perevod_status) FROM perevody WHERE perevod_status = 'false' AND 
                user_get = '{i['user_id']}';""")
            get = cursor.fetchall()


            return jsonify({'id': i['user_id'], 'user_name': i['user_name'], 'balance': i['balance'],
                            'money': i['money'], 'my_money': i['my_money'], 'status_count': get[0]['COUNT(perevod_status)'],
                            "status": i['my_businesses']})
    return jsonify('Неверный логин или пароль')
@app.route('/my_profile', methods=['POST']) #
def my_profile():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id']

    }
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_name, phone_numb, email, admin_status FROM users WHERE user_id = '{user_request['id']}';""")
    get = cursor.fetchall()


    return jsonify(get)


@app.route('/date_operations', methods=['POST']) #
def date_operations():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id'],
        'name': request_data['name'],
        'date': request_data['date']

    }



    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT {user_request['name']} as user_get, perevody.summa, perevody.data, users.user_name as user_send, users.user_del_status 
        FROM perevody INNER JOIN users ON 
        perevody.user_send = users.user_id WHERE perevody.user_get = '{user_request['id']}' 
        AND data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")

    perevody_user_get = cursor.fetchall()


    cursor.execute(
        f"""SELECT users.user_name as user_get, perevody.summa, perevody.data, {user_request['name']} as user_send, users.user_del_status
            FROM perevody INNER JOIN users ON
            perevody.user_get = users.user_id WHERE perevody.user_send = '{user_request['id']}'
            AND data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")

    perevody_user_send = cursor.fetchall()



    res = []
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, istochnik, istochnik_del_status,
            position, position_del_status FROM operations
            JOIN users USING (user_id)
            JOIN b_businesses USING (id_business)
            JOIN b_istochniky USING (id_istochnik)
            JOIN b_positions USING (id_position)
            WHERE user_id = '{user_request['id']}'
            AND data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
            ;""")

    prihod = cursor.fetchall()

    for i in prihod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, cel, cely_del_status,
                position, position_del_status FROM operations
                JOIN users USING (user_id)
                JOIN b_businesses USING (id_business)
                JOIN b_cely USING (id_cel)
                JOIN b_positions USING (id_position)
                WHERE user_id = '{user_request['id']}'
                AND data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                ;""")
    rashod = cursor.fetchall()

    for i in rashod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type FROM operations
                   JOIN users USING (user_id)
                   WHERE user_id = '{user_request['id']}' AND
                   (operation_type = 'Собственные внесено' OR operation_type = 'Собственные выведено')
                    AND data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")
    sobstven = cursor.fetchall()
    for i in sobstven:
        res.append(i)




    return jsonify({'perevody_get': perevody_user_get, 'perevody_send': perevody_user_send, 'operations': res})





@app.route('/prihod/business', methods=['GET']) # такое же к расходу
def prihod_business():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business FROM b_businesses WHERE status <> 'false' and business_del_status <> 'true';""")
    businesses = cursor.fetchall()

    return jsonify(businesses)
@app.route('/prihod/business_add', methods=['POST']) ## cюда же запрос в расходах при добавлении бизнеса #
def prihod_add_business():
    request_data = request.get_json()
    user_request = {
        'add_business': request_data['add_business']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""INSERT INTO b_businesses (business, business_del_status, status) VALUES ('{user_request['add_business']}',
        'false',
        'true');""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM b_businesses WHERE status <> 'false' and business_del_status <> 'true';""")
    businesses = cursor.fetchall()

    return jsonify(businesses)
@app.route('/prihod/istochnik', methods=['GET']) #
def prihod_istochnik():

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik, istochnik FROM b_istochniky WHERE status <> 'false' and istochnik_del_status <> 'true'""")
    istochniky = cursor.fetchall()


    return jsonify(istochniky)
@app.route('/prihod/add_istochnik', methods=['POST']) #
def prihod_add_istochnik():
    request_data = request.get_json()
    user_request = {
        'add_istochnik': request_data['add_istochnik']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT istochnik FROM b_istochniky
                          WHERE istochnik = '{user_request['add_istochnik']}';""")
    istochnik = cursor.fetchall()
    if istochnik:
        return jsonify('Такой источник уже существует')
    else:
        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO b_istochniky (istochnik, istochnik_del_status, status) 
                           VALUES ('{user_request['add_istochnik']}', 'false', 'true')""")
        g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT id_istochnik, istochnik FROM b_istochniky WHERE status <> 'false' and istochnik_del_status <> 'true';""")
        cely = cursor.fetchall()

        return jsonify(cely)
@app.route('/prihod/position', methods=['GET']) ## тот же запрос для расходов  #
def prihod_position():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_position, position FROM b_positions WHERE status <> 'false' AND position_del_status <> 'true'""")
    positions = cursor.fetchall()

    return jsonify(positions)
@app.route('/prihod/position_add', methods=['POST']) ## запрос добаления позиция для расходов #
def prihod_position_add():
    request_data = request.get_json()
    user_request = {
        'add_position': request_data['add_position']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""INSERT INTO b_positions (position, position_del_status, status) VALUES ('{user_request['add_position']}',
        'false',
        'true');""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_position, position FROM b_positions WHERE status <> 'false' AND position_del_status <> 'true'""")
    positions = cursor.fetchall()

    return jsonify(positions)
@app.route('/prihod/prihod_enter', methods=['POST']) #
def prihod_prihod_enter():
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
        f"""SELECT balance, money FROM users WHERE user_id = '{user_request['id']}';""")
    get = cursor.fetchall()


    get[0]['balance'] += user_request['sum']
    get[0]['money'] += user_request['sum']

    cursor = get_db().cursor()
    cursor.execute(
        f"""UPDATE users SET balance = '{get[0]['balance']}', money = '{get[0]['money']}' 
        WHERE user_id = '{user_request['id']}';""")
    g.db.commit()



    cursor.execute(
    f"""INSERT INTO operations (user_id, summa, data, operation_type, id_business, id_istochnik, id_position)
    VALUES ('{user_request['id']}', '{user_request['sum']}', '{user_request['time']}', 'Приход',
    '{user_request['businesses']}', '{user_request['istochnik_prihoda']}', '{user_request['position']}');""")
    g.db.commit()


    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, 
        (SELECT COUNT(perevody.perevod_status)
          FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = '{user_request['id']}') 
          AS perevod_status FROM users WHERE user_id = {user_request['id']};""")

    get = cursor.fetchall()

    return jsonify({'id': user_request['id'], 'user_name': get[0]['user_name'], 'balance': get[0]['balance'],
                    'money': get[0]['money'], 'my_money': get[0]['my_money'], 'status_count': get[0]['perevod_status'],
                    "status": get[0]['my_businesses']}, {'id': user_request['id'], 'sum': user_request['sum'],
                    'time': user_request['time'], 'businesses': user_request['businesses'],
                    'istochnik_prihoda':user_request['istochnik_prihoda'], 'position': user_request['position'],
                    'operation_type': 'Приход'})



@app.route('/rashod/cel', methods=['POST']) #
def rashod_cel():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_cel, cel FROM b_cely WHERE status <> 'false' and cely_del_status <> 'true';""")
    cely = cursor.fetchall()

    return jsonify(cely)
@app.route('/rashod/add_cel', methods=['POST']) #
def add_cel():
    request_data = request.get_json()
    user_request = {
        'add_cel': request_data['add_cel']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT cel FROM b_cely
                      WHERE cel = '{user_request['add_cel']}';""")
    cel = cursor.fetchall()
    if cel:
        return jsonify('Такая цель уже существует')
    else:
        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO b_cely (cel, cely_del_status, status) 
                       VALUES ('{user_request['add_cel']}', 'false', 'true')""")
        g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT id_cel, cel FROM b_cely WHERE status <> 'false' and cely_del_status <> 'true';""")
        cely = cursor.fetchall()

        return jsonify(cely)
@app.route('/rashod/enter', methods=['POST']) #
def rashod_enter():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id'],
        'sum': request_data['sum'],
        'time': request_data['time'],
        'businesses': request_data['businesses'],
        'сel_rashoda': request_data['сel_rashoda'],
        'position': request_data['position']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, money FROM users WHERE user_id = '{user_request['id']}';""")
    get = cursor.fetchall()

    get[0]['balance'] -= user_request['sum']
    get[0]['money'] -= user_request['sum']

    cursor = get_db().cursor()
    cursor.execute(
        f"""UPDATE users SET balance = '{get[0]['balance']}', money = '{get[0]['money']}' 
            WHERE user_id = '{user_request['id']}';""")
    g.db.commit()

    cursor.execute(
        f"""INSERT INTO operations (user_id, summa, data, operation_type, id_business, id_cel, id_position)
        VALUES ('{user_request['id']}', '{user_request['sum']}', '{user_request['time']}', 'Расход',
        '{user_request['businesses']}', '{user_request['сel_rashoda']}', '{user_request['position']}');""")
    g.db.commit()

    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, 
            (SELECT COUNT(perevody.perevod_status)
              FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = '{user_request['id']}') 
              AS perevod_status FROM users WHERE user_id = {user_request['id']};""")

    get = cursor.fetchall()

    return jsonify({'id': user_request['id'], 'user_name': get[0]['user_name'], 'balance': get[0]['balance'],
                    'money': get[0]['money'], 'my_money': get[0]['my_money'], 'status_count': get[0]['perevod_status'],
                    "status": get[0]['my_businesses']}, {'id': user_request['id'], 'sum': user_request['sum'],
                                                         'time': user_request['time'],
                                                         'businesses': user_request['businesses'],
                                                         'сel_rashoda': user_request['сel_rashoda'],
                                                         'position': user_request['position'],
                                                         'operation_type': 'Расход'})




@app.route('/sobstv/vnos', methods=['POST']) #
def sobstv_vnos():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'sum': request_data['sum'],
        'time': request_data['time']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, my_money FROM users WHERE user_id = '{user_request['id']}';""")
    get = cursor.fetchall()
    get[0]['balance'] += user_request['sum']
    get[0]['my_money'] += user_request['sum']

    cursor.execute(
        f"""UPDATE users SET balance = '{get[0]['balance']}', my_money = '{get[0]['my_money']}' 
        WHERE user_id = '{user_request['id']}';""")
    g.db.commit()

    cursor.execute(
        f"""INSERT INTO operations (user_id, summa, data, operation_type)
          VALUES ('{user_request['id']}', '{user_request['sum']}', '{user_request['time']}', 'Собственные внесено');""")
    g.db.commit()



    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, 
        (SELECT COUNT(perevody.perevod_status) 
        FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = '{user_request['id']}') 
        AS perevod_status FROM users WHERE user_id = {user_request['id']};""")

    get = cursor.fetchall()


    return jsonify({'id': user_request['id'], 'user_name': get[0]['user_name'], 'balance': get[0]['balance'],
                    'money': get[0]['money'], 'my_money': get[0]['my_money'], 'status_count': get[0]['perevod_status'],
                    "status": get[0]['my_businesses']}, {'id': user_request['id'], 'sum': user_request['sum'],
                    'time': user_request['time'], 'operation_type': 'Собственные внесено'})
@app.route('/sobstv/vivod', methods=['POST']) #
def sobstv_vivod():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'sum': request_data['sum'],
        'time': request_data['time']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, my_money FROM users WHERE user_id = '{user_request['id']}';""")
    get = cursor.fetchall()
    get[0]['balance'] -= user_request['sum']
    get[0]['my_money'] -= user_request['sum']

    cursor.execute(
        f"""UPDATE users SET balance = '{get[0]['balance']}', my_money = '{get[0]['my_money']}' 
        WHERE user_id = '{user_request['id']}';""")
    g.db.commit()

    cursor.execute(
        f"""INSERT INTO operations (user_id, summa, data, operation_type)
          VALUES ('{user_request['id']}', '{user_request['sum']}', '{user_request['time']}', 'Собственные выведено');""")
    g.db.commit()


    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, (SELECT COUNT(perevody.perevod_status) 
        FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = {user_request['id']}) 
        AS perevod_status FROM users WHERE user_id = {user_request['id']};""")

    get = cursor.fetchall()


    return jsonify({'id': user_request['id'], 'user_name': get[0]['user_name'], 'balance': get[0]['balance'],
                    'money': get[0]['money'], 'my_money': get[0]['my_money'], 'status_count': get[0]['perevod_status'],
                    "status": get[0]['my_businesses']}, {'id': user_request['id'], 'sum': user_request['sum'],
                    'time': user_request['time'], 'operation_type': 'Собственные выведено'})




@app.route('/perevod/info', methods=['POST']) #
def perevod_info():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_id, user_name FROM users WHERE user_id != '{user_request['id']}' AND user_del_status <> 'true'
    AND status <> 'false'""")
    get = cursor.fetchall()


    return jsonify(get)
@app.route('/perevod/enter', methods=['POST']) #
def perevod_enter():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'send_id': request_data['send_id'],
        'sum': request_data['sum'],
        'data': request_data['data'],
        'send_user': request_data['send_user']


    }



    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, my_money FROM users WHERE user_id = '{user_request['id']}';""")
    get = cursor.fetchall()
    get[0]['balance'] -= user_request['sum']
    get[0]['my_money'] -= user_request['sum']



    cursor.execute(
        f"""UPDATE users SET balance = '{get[0]['balance']}', my_money = '{get[0]['my_money']}' WHERE user_id = '{user_request['id']}';""")
    g.db.commit()


    cursor.execute(
        f"""INSERT INTO perevody (user_get, summa, data, user_send, perevod_status)
          VALUES ('{user_request['send_id']}', '{user_request['sum']}', '{user_request['data']}', '{user_request['id']}',
          'false');""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, (SELECT COUNT(perevody.perevod_status)
        FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = '{user_request['id']}') AS perevod_status 
        FROM users WHERE user_id = {user_request['id']};""")

    to_page = cursor.fetchall()

    return jsonify({'id': user_request['id'], 'user_name': to_page[0]['user_name'], 'balance': to_page[0]['balance'],
                    'money': to_page[0]['money'], 'my_money': to_page[0]['my_money'], 'status_count': to_page[0]['perevod_status'],
                    "status": to_page[0]['my_businesses']}, {'businesses': 'Кому' + ' ' + user_request['send_user'], 'sum': user_request['sum'],
                                                         'time': user_request['data'],
                                                         'operation_type': 'Перевод'})



@app.route('/perevod/show', methods=['POST']) #
def perevod_show():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id']
    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT perevody.summa, perevody.data, users.user_name, users.user_id FROM perevody INNER JOIN users ON 
        perevody.user_send = users.user_id WHERE perevody.user_get = '{user_request['id']}' AND 
        perevody.perevod_status = 'false';""")

    get = cursor.fetchall()


    return jsonify(get)
@app.route('/perevod/confirm', methods=['POST']) #
def perevod_confirm():
    request_data = request.get_json()

    user_request = {
        'id': request_data['id'],
        'info': request_data['info']

    }


    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT balance, my_money FROM users WHERE user_id = '{user_request['id']}';""")
    get = cursor.fetchall()

    balance = get[0]['balance']
    my_money = get[0]['my_money']

    for i in user_request['info']:
        balance += i['summa']
        my_money += i['summa']

        cursor.execute(
            f"""UPDATE perevody SET perevod_status = '{i['status']}' WHERE user_get = '{user_request['id']}' AND
            summa = '{i['summa']}' AND data = '{i['data']}' AND user_send = '{i['send_id']}';""")
        g.db.commit()

    cursor.execute(
        f"""UPDATE users SET balance = '{balance}', my_money = '{my_money}' WHERE user_id = '{user_request['id']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT users.user_name, users.balance, users.money, users.my_money, users.my_businesses, (SELECT COUNT(perevody.perevod_status) 
          FROM perevody WHERE perevody.perevod_status = 'false' AND user_get = '{user_request['id']}') AS 
          perevod_status FROM users WHERE user_id = {user_request['id']};""")

    to_page = cursor.fetchall()


    res = []
    for i in user_request['info']:
        res.extend([{'name': 'От' + ' ' + i['user_name'], 'sum': i['summa'],
                    'time': i['data'],
                    'operation_type': 'Перевод'}])



    return jsonify({'id': user_request['id'], 'user_name': to_page[0]['user_name'], 'balance': to_page[0]['balance'],
                    'money': to_page[0]['money'], 'my_money': to_page[0]['my_money'],
                    'status_count': to_page[0]['perevod_status'],
                    "status": to_page[0]['my_businesses']}, res)




@app.route('/spravochniki/business', methods=['GET']) #
def spravochniki_business():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM b_businesses WHERE business_del_status <> 'true';""")
    businesses = cursor.fetchall()

    return jsonify(businesses)
@app.route('/business/add', methods=['POST']) #
def business_add():
    request_data = request.get_json()
    user_request = {
        'bussiness_name': request_data['bussiness_name'],
        'status': request_data['status']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""INSERT INTO b_businesses (business, business_del_status, status) 
        VALUES ('{user_request['bussiness_name']}', 'false', '{user_request['status']}')""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM b_businesses WHERE business_del_status <> 'true';""")
    businesses = cursor.fetchall()

    return jsonify(businesses)
@app.route('/bussinesses/rename', methods=['POST']) #
def bussinesses_rename():
    request_data = request.get_json()
    user_request = {
        'id_business': request_data['id_business'],
        'new_name': request_data['new_name'],
        'status': request_data['status']
    }

    cursor = get_db().cursor()

    cursor.execute(
        f"""UPDATE b_businesses SET business = '{user_request['new_name']}',
                        status = '{user_request['status']}'
                        WHERE id_business = '{user_request['id_business']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM b_businesses WHERE business_del_status <> 'true';""")
    businesses = cursor.fetchall()

    return jsonify(businesses)
@app.route('/bussinesses/del', methods=['POST']) #
def bussinesses_del():
    request_data = request.get_json()
    user_request = {
        'id_business': request_data['id_business']

    }
    cursor = get_db().cursor()
    cursor.execute(
        f"""UPDATE b_businesses SET business_del_status = 'true'
                           WHERE id_business = '{user_request['id_business']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM b_businesses WHERE business_del_status <> 'true';""")
    businesses = cursor.fetchall()

    return jsonify(businesses)




@app.route('/spravochniki/istochniky', methods=['GET']) #
def istochnik_prihoda():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik, istochnik, status FROM b_istochniky WHERE istochnik_del_status <> 'true';""")
    istochniky = cursor.fetchall()

    return jsonify(istochniky)
@app.route('/spravochniki/istochniky/edit_confirm', methods=['POST']) #
def istoch_edit_confirm():
    request_data = request.get_json()
    user_request = {
        'id_istochnik': request_data['id_istochnik'],
        'new_name': request_data['new_name'],
        'status': request_data['status']
    }

    cursor = get_db().cursor()

    cursor.execute(
        f"""UPDATE b_istochniky SET istochnik = '{user_request['new_name']}',
                           status = '{user_request['status']}'
                           WHERE id_istochnik = '{user_request['id_istochnik']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik, istochnik, status FROM b_istochniky WHERE istochnik_del_status <> 'true';""")
    istochniky = cursor.fetchall()

    return jsonify(istochniky)
@app.route('/spravochniki/istochniky/add_confirm', methods=['POST']) #
def istochnik_add_confirm():
    request_data = request.get_json()
    user_request = {
        'istochnik_name': request_data['istochnik_name'],
        'status': request_data['status']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT istochnik FROM b_istochniky
               WHERE istochnik = '{user_request['istochnik_name']}';""")
    istochnik = cursor.fetchall()
    if istochnik:
        return jsonify('Такой источник уже существует')
    else:
        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO b_istochniky (istochnik, istochnik_del_status, status) 
                VALUES ('{user_request['istochnik_name']}', 'false', '{user_request['status']}')""")
        g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT id_istochnik, istochnik, status FROM b_istochniky WHERE istochnik_del_status <> 'true';""")
        istochniky = cursor.fetchall()

        return jsonify(istochniky)
@app.route('/spravochniki/istochniky/del', methods=['POST']) #
def istochniky_del():
    request_data = request.get_json()
    user_request = {

        'istochnik_id': request_data['istochnik_id']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""UPDATE b_istochniky SET istochnik_del_status = 'true'
                               WHERE id_istochnik = '{user_request['istochnik_id']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_istochnik, istochnik, status FROM b_istochniky WHERE istochnik_del_status <> 'true';""")
    istochniky = cursor.fetchall()

    return jsonify(istochniky)








@app.route('/spravochniki/cely', methods=['GET']) #
def cel_rashoda():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_cel, cel, status FROM b_cely WHERE cely_del_status <> 'true';""")
    cely = cursor.fetchall()

    return jsonify(cely)
@app.route('/spravochniki/cel/edit_confirm', methods=['POST']) #
def cel_edit_confirm():
    request_data = request.get_json()
    user_request = {
        'id_cel': request_data['id_cel'],
        'new_name': request_data['new_name'],
        'status': request_data['status']
    }

    cursor = get_db().cursor()

    cursor.execute(
        f"""UPDATE b_cely SET cel = '{user_request['new_name']}',
                               status = '{user_request['status']}'
                               WHERE id_cel = '{user_request['id_cel']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_cel, cel, status FROM b_cely WHERE cely_del_status <> 'true';""")
    cely = cursor.fetchall()

    return jsonify(cely)
@app.route('/spravochniki/cel/add_confirm', methods=['POST']) #
def cel_add_confirm():
    request_data = request.get_json()
    user_request = {
        'cel_name': request_data['cel_name'],
        'status': request_data['status']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT cel FROM b_cely
                   WHERE cel = '{user_request['cel_name']}';""")
    istochnik = cursor.fetchall()
    if istochnik:
        return jsonify('Такая цель уже существует')
    else:
        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO b_cely (cel, cely_del_status, status) 
                    VALUES ('{user_request['cel_name']}', 'false', '{user_request['status']}')""")
        g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT id_cel, cel, status FROM b_cely WHERE cely_del_status <> 'true';""")
        cely = cursor.fetchall()

        return jsonify(cely)
@app.route('/spravochniki/cel/del', methods=['POST']) #
def cel_del():
    request_data = request.get_json()
    user_request = {

        'cel_id': request_data['cel_id']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""UPDATE b_cely SET cely_del_status = 'true'
                                   WHERE id_cel = '{user_request['cel_id']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_cel, cel, status FROM b_cely WHERE cely_del_status <> 'true';""")
    cely = cursor.fetchall()

    return jsonify(cely)






@app.route('/spravochniki/positions', methods=['GET']) #
def positions():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_position, position, status FROM b_positions WHERE position_del_status <> 'true';""")
    position = cursor.fetchall()

    return jsonify(position)
@app.route('/spravochniki/position/edit_confirm', methods=['POST']) #
def position_edit_confirm():
    request_data = request.get_json()
    user_request = {
        'id_position': request_data['id_position'],
        'new_name': request_data['new_name'],
        'status': request_data['status']
    }

    cursor = get_db().cursor()

    cursor.execute(
        f"""UPDATE b_positions SET position = '{user_request['new_name']}',
                                  status = '{user_request['status']}'
                                  WHERE id_position = '{user_request['id_position']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_position, position, status FROM b_positions WHERE position_del_status <> 'true';""")
    position = cursor.fetchall()

    return jsonify(position)
@app.route('/spravochniki/position/add_confirm', methods=['POST']) #
def position_add_confirm():
    request_data = request.get_json()
    user_request = {
        'position_name': request_data['position_name'],
        'status': request_data['status']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT position FROM b_positions
                     WHERE position = '{user_request['position_name']}';""")
    position = cursor.fetchall()
    if position:
        return jsonify('Такая позиция уже существует')
    else:
        cursor = get_db().cursor()
        cursor.execute(
            f"""INSERT INTO b_positions (position, position_del_status, status) 
                      VALUES ('{user_request['position_name']}', 'false', '{user_request['status']}')""")
        g.db.commit()

        cursor = get_db().cursor()
        cursor.execute(
            f"""SELECT id_position, position, status FROM b_positions WHERE position_del_status <> 'true';""")
        position = cursor.fetchall()

        return jsonify(position)
@app.route('/spravochniki/position/del', methods=['POST']) #
def position_del():
    request_data = request.get_json()
    user_request = {

        'position_id': request_data['position_id']

    }

    cursor = get_db().cursor()
    cursor.execute(
        f"""UPDATE b_positions SET position_del_status = 'true'
                                     WHERE id_position = '{user_request['position_id']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_position, position, status FROM b_positions WHERE position_del_status <> 'true';""")
    position = cursor.fetchall()

    return jsonify(position)



@app.route('/spravochniki/users', methods=['GET']) #
def spravochniki_users():
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_id, user_name, admin_status, phone_numb, email, status FROM users WHERE user_del_status <> 'true';""")
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
                           WHERE user_id = '{user_request['id']}'""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_id, user_name, admin_status, phone_numb, email, status FROM users WHERE user_del_status <> 'true';""")
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
        'password': request_data['password']

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
            f"""SELECT user_id, user_name, admin_status, phone_numb, email, status FROM users WHERE user_del_status <> 'true';""")
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
        f"""UPDATE users SET user_del_status = 'true'
                                         WHERE user_id = '{user_request['user_id']}';""")
    g.db.commit()

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_id, user_name, admin_status, phone_numb, email, status FROM users WHERE user_del_status <> 'true';""")
    users = cursor.fetchall()

    return jsonify(users)




### раздел с корзиной
@app.route('/spravochniki/basket', methods=['GET']) #
def spravochniki_basket():

    res = []

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM b_businesses
        
         WHERE business_del_status = 'true';""")
    businesses = cursor.fetchall()

    for i in businesses:
        res.append(i)

    cursor.execute(
        f"""SELECT id_cel, cel, status FROM b_cely
        WHERE cely_del_status = 'true'""")
    cely = cursor.fetchall()

    for i in cely:
        res.append(i)

    cursor.execute(
        f"""SELECT id_istochnik, istochnik, status FROM b_istochniky
           WHERE istochnik_del_status = 'true'""")
    istochniky = cursor.fetchall()

    for i in istochniky:
        res.append(i)

    cursor.execute(
        f"""SELECT id_position, position, status FROM b_positions
               WHERE position_del_status = 'true'""")
    positions = cursor.fetchall()

    for i in positions:
        res.append(i)


    return jsonify(res)
@app.route('/spravochniki/basket/del_status', methods=['GET'])
def spravochniki_basket_del_status():
    request_data = request.get_json()
    user_request = {
        'ids': request_data['ids'],
        'del_status': request_data['del_status']
    }

    for i in user_request['ids']:
        for e in user_request['ids'][i]:
            if i == 'id_business':
                cursor = get_db().cursor()
                cursor.execute(
                    f"""UPDATE b_businesses SET business_del_status = '{user_request['del_status']}'
                        WHERE id_business = '{e}'""")
                g.db.commit()
            elif i == 'id_istochnik':
                cursor = get_db().cursor()
                cursor.execute(
                    f"""UPDATE b_istochniky SET istochnik_del_status = '{user_request['del_status']}'
                                       WHERE id_istochnik = '{e}'""")
                g.db.commit()
            elif i == 'id_cel':
                cursor = get_db().cursor()
                cursor.execute(
                    f"""UPDATE b_cely SET cely_del_status = '{user_request['del_status']}'
                                                       WHERE id_cel = '{e}'""")
                g.db.commit()
            elif i == 'id_position':
                cursor = get_db().cursor()
                cursor.execute(
                    f"""UPDATE b_positions SET position_del_status = '{user_request['del_status']}'
                                                                      WHERE id_position = '{e}'""")
                g.db.commit()

    res = []

    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT id_business, business, status FROM b_businesses

             WHERE business_del_status = 'true';""")
    businesses = cursor.fetchall()

    for i in businesses:
        res.append(i)

    cursor.execute(
        f"""SELECT id_cel, cel, status FROM b_cely
            WHERE cely_del_status = 'true'""")
    cely = cursor.fetchall()

    for i in cely:
        res.append(i)

    cursor.execute(
        f"""SELECT id_istochnik, istochnik, status FROM b_istochniky
               WHERE istochnik_del_status = 'true'""")
    istochniky = cursor.fetchall()

    for i in istochniky:
        res.append(i)

    cursor.execute(
        f"""SELECT id_position, position, status FROM b_positions
                   WHERE position_del_status = 'true'""")
    positions = cursor.fetchall()

    for i in positions:
        res.append(i)

    return jsonify(res)
###########




@app.route('/otchety_balacne', methods=['POST']) #
def otchety_balacne():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'date': request_data['date']

    }



    res = []
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, istochnik, istochnik_del_status,
                position, position_del_status FROM operations
                JOIN users USING (user_id)
                JOIN b_businesses USING (id_business)
                JOIN b_istochniky USING (id_istochnik)
                JOIN b_positions USING (id_position)
                WHERE
                data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                ;""")

    prihod = cursor.fetchall()



    for i in prihod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, cel, cely_del_status,
                    position, position_del_status FROM operations
                    JOIN users USING (user_id)
                    JOIN b_businesses USING (id_business)
                    JOIN b_cely USING (id_cel)
                    JOIN b_positions USING (id_position)
                    WHERE
                    data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                    ;""")
    rashod = cursor.fetchall()

    for i in rashod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type FROM operations
                       JOIN users USING (user_id)
                       WHERE
                       data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}' AND
                       operation_type IN ('Собственные внесено', 'Собственные выведено')
                       """)
    sobstv = cursor.fetchall()

    for i in sobstv:
        res.append(i)

    cursor.execute(
        f"""SELECT perevody.user_get, perevody.summa, perevody.data, perevody.user_send,
        users.user_name as user_send_name, users.user_del_status
        FROM perevody INNER JOIN users ON
        perevody.user_send = users.user_id
        WHERE data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")
    perevody = cursor.fetchall()

    for i in perevody:
        cursor.execute(
            f"""SELECT user_name FROM users
            WHERE user_id = '{i['user_get']}'""")
        name = cursor.fetchall()
        i.setdefault('user_get_name', name[0]['user_name'])

    prihod = 0
    rashod = 0

    for i in res:
        if i['operation_type'] == 'Приход' and i['user_id'] == user_request['id']:
            prihod += i['summa']
        if i['operation_type'] == 'Расход' and i['user_id'] == user_request['id']:
            rashod += i['summa']

    itog = prihod - rashod

    grafic_perevod_get = []
    grafic_perevod_send = []
    grafic_operations = []

    for i in perevody:
        if i['user_get'] == user_request['id']:
            grafic_perevod_get.extend([{'summa': i['summa'], 'data': i['data']}])
        if i['user_send'] == user_request['id']:
            grafic_perevod_send.extend([{'summa': i['summa'], 'data': i['data']}])

    for i in res:
        if i['user_id'] == user_request['id']:
            grafic_operations.extend([{'summa': i['summa'], 'data': i['data'], 'operation_type': i['operation_type']}])


    return jsonify({'itog': itog, 'prihod': prihod, 'rashod': rashod, 'grafic': {'user_get': grafic_perevod_get,
    'user_send': grafic_perevod_send, 'operations': grafic_operations}, 'table': {'perevody': perevody,
    'operations': res}})
@app.route('/otchety_business', methods=['POST'])
def otchety_business():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'business': request_data['business'],
        'date': request_data['date']
    }
    res = []
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, istochnik, istochnik_del_status,
                    position, position_del_status FROM operations
                    JOIN users USING (user_id)
                    JOIN b_businesses USING (id_business)
                    JOIN b_istochniky USING (id_istochnik)
                    JOIN b_positions USING (id_position)
                    WHERE
                    data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                    ;""")

    prihod = cursor.fetchall()


    for i in prihod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, cel, cely_del_status,
                        position, position_del_status FROM operations
                        JOIN users USING (user_id)
                        JOIN b_businesses USING (id_business)
                        JOIN b_cely USING (id_cel)
                        JOIN b_positions USING (id_position)
                        WHERE 
                        data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                        ;""")
    rashod = cursor.fetchall()

    for i in rashod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type FROM operations
                           JOIN users USING (user_id)
                           WHERE 
                           data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}' AND 
                           operation_type IN ('Собственные внесено', 'Собственные выведено')
                           """)
    sobstv = cursor.fetchall()

    for i in sobstv:
        res.append(i)

    cursor.execute(
        f"""SELECT perevody.user_get, perevody.summa, perevody.data, perevody.user_send, 
            users.user_name as user_send_name, users.user_del_status
            FROM perevody INNER JOIN users ON
            perevody.user_send = users.user_id
            WHERE data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")
    perevody = cursor.fetchall()

    for i in perevody:
        cursor.execute(
            f"""SELECT user_name FROM users
                WHERE user_id = '{i['user_get']}'""")
        name = cursor.fetchall()
        i.setdefault('user_get_name', name[0]['user_name'])

    prihod = 0
    rashod = 0

    for i in res:
        if i['operation_type'] == 'Приход' and i['user_id'] == user_request['id'] and i['business'] == user_request['business']:
            prihod += i['summa']
        if i['operation_type'] == 'Расход' and i['user_id'] == user_request['id'] and i['business'] == user_request['business']:
            rashod += i['summa']

    itog = prihod - rashod

    grafic_perevod_get = []
    grafic_perevod_send = []
    grafic_operations = []

    for i in perevody:
        if i['user_get'] == user_request['id']:
            grafic_perevod_get.extend([{'summa': i['summa'], 'data': i['data']}])
        if i['user_send'] == user_request['id']:
            grafic_perevod_send.extend([{'summa': i['summa'], 'data': i['data']}])

    for i in res:
        if i['user_id'] == user_request['id']:
            grafic_operations.extend([{'summa': i['summa'], 'data': i['data'], 'operation_type': i['operation_type']}])

    return jsonify({'itog': itog, 'prihod': prihod, 'rashod': rashod, 'grafic': {'user_get': grafic_perevod_get,
                                                                                 'user_send': grafic_perevod_send,
                                                                                 'operations': grafic_operations},
                    'table': {'perevody': perevody,
                              'operations': res}})
@app.route('/otchety_istochnik', methods=['POST'])
def otchety_istochnik():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'date': request_data['date'],
        'istochnik': request_data['istochnik'],

    }

    res = []
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, istochnik, istochnik_del_status,
                       position, position_del_status FROM operations
                       JOIN users USING (user_id)
                       JOIN b_businesses USING (id_business)
                       JOIN b_istochniky USING (id_istochnik)
                       JOIN b_positions USING (id_position)
                       WHERE
                       data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                       ;""")

    prihod = cursor.fetchall()

    for i in prihod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, cel, cely_del_status,
                           position, position_del_status FROM operations
                           JOIN users USING (user_id)
                           JOIN b_businesses USING (id_business)
                           JOIN b_cely USING (id_cel)
                           JOIN b_positions USING (id_position)
                           WHERE 
                           data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                           ;""")
    rashod = cursor.fetchall()

    for i in rashod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type FROM operations
                              JOIN users USING (user_id)
                              WHERE 
                              data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}' AND 
                              operation_type IN ('Собственные внесено', 'Собственные выведено')
                              """)
    sobstv = cursor.fetchall()

    for i in sobstv:
        res.append(i)

    cursor.execute(
        f"""SELECT perevody.user_get, perevody.summa, perevody.data, perevody.user_send, 
               users.user_name as user_send_name, users.user_del_status
               FROM perevody INNER JOIN users ON
               perevody.user_send = users.user_id
               WHERE data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")
    perevody = cursor.fetchall()

    for i in perevody:
        cursor.execute(
            f"""SELECT user_name FROM users
                   WHERE user_id = '{i['user_get']}'""")
        name = cursor.fetchall()
        i.setdefault('user_get_name', name[0]['user_name'])

    prihod_all = 0
    prihod_istochnik = 0

    for i in res:
        if i['operation_type'] == 'Приход' and i['user_id'] == user_request['id']:
            prihod_all += i['summa']
        if i['operation_type'] == 'Приход' and i['user_id'] == user_request['id'] and i['istochnik'] == user_request[
            'istochnik']:
            prihod_istochnik += i['summa']

    ostatok = round(prihod_istochnik / prihod_all * 100, 2)


    grafic_perevod_get = []
    grafic_perevod_send = []
    grafic_operations = []

    for i in perevody:
        if i['user_get'] == user_request['id']:
            grafic_perevod_get.extend([{'summa': i['summa'], 'data': i['data']}])
        if i['user_send'] == user_request['id']:
            grafic_perevod_send.extend([{'summa': i['summa'], 'data': i['data']}])

    for i in res:
        if i['user_id'] == user_request['id']:
            grafic_operations.extend([{'summa': i['summa'], 'data': i['data'], 'operation_type': i['operation_type']}])

    return jsonify({'ostatok': ostatok, 'prihod_all': prihod_all, 'prihod_istochnik': prihod_istochnik,
                    'grafic': {'user_get': grafic_perevod_get,
                    'user_send': grafic_perevod_send,
                    'operations': grafic_operations},
                    'table': {'perevody': perevody, 'operations': res}})
@app.route('/otchety_cel', methods=['POST'])
def otchety_cel():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'date': request_data['date'],
        'cel': request_data['cel'],

    }

    res = []
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, istochnik, istochnik_del_status,
                          position, position_del_status FROM operations
                          JOIN users USING (user_id)
                          JOIN b_businesses USING (id_business)
                          JOIN b_istochniky USING (id_istochnik)
                          JOIN b_positions USING (id_position)
                          WHERE
                          data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                          ;""")

    prihod = cursor.fetchall()

    for i in prihod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, cel, cely_del_status,
                              position, position_del_status FROM operations
                              JOIN users USING (user_id)
                              JOIN b_businesses USING (id_business)
                              JOIN b_cely USING (id_cel)
                              JOIN b_positions USING (id_position)
                              WHERE 
                              data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                              ;""")
    rashod = cursor.fetchall()

    for i in rashod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type FROM operations
                                 JOIN users USING (user_id)
                                 WHERE 
                                 data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}' AND 
                                 operation_type IN ('Собственные внесено', 'Собственные выведено')
                                 """)
    sobstv = cursor.fetchall()

    for i in sobstv:
        res.append(i)

    cursor.execute(
        f"""SELECT perevody.user_get, perevody.summa, perevody.data, perevody.user_send, 
                  users.user_name as user_send_name, users.user_del_status
                  FROM perevody INNER JOIN users ON
                  perevody.user_send = users.user_id
                  WHERE data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")
    perevody = cursor.fetchall()

    for i in perevody:
        cursor.execute(
            f"""SELECT user_name FROM users
                      WHERE user_id = '{i['user_get']}'""")
        name = cursor.fetchall()
        i.setdefault('user_get_name', name[0]['user_name'])

    cel_all = 0
    prihod_cel = 0

    for i in res:
        if i['operation_type'] == 'Расход' and i['user_id'] == user_request['id']:
            cel_all += i['summa']
        if i['operation_type'] == 'Расход' and i['user_id'] == user_request['id'] and i['cel'] == user_request[
            'cel']:
            prihod_cel += i['summa']

    ostatok = round(prihod_cel / cel_all * 100, 2)

    grafic_perevod_get = []
    grafic_perevod_send = []
    grafic_operations = []

    for i in perevody:
        if i['user_get'] == user_request['id']:
            grafic_perevod_get.extend([{'summa': i['summa'], 'data': i['data']}])
        if i['user_send'] == user_request['id']:
            grafic_perevod_send.extend([{'summa': i['summa'], 'data': i['data']}])

    for i in res:
        if i['user_id'] == user_request['id']:
            grafic_operations.extend([{'summa': i['summa'], 'data': i['data'], 'operation_type': i['operation_type']}])

    return jsonify({'ostatok': ostatok, 'cel_all': cel_all, 'prihod_cel': prihod_cel,
                    'grafic': {'user_get': grafic_perevod_get,
                               'user_send': grafic_perevod_send,
                               'operations': grafic_operations},
                    'table': {'perevody': perevody, 'operations': res}})
@app.route('/otchety_users', methods=['POST'])
def otchety_users():
    request_data = request.get_json()
    user_request = {
        'id': request_data['id'],
        'req_user': request_data['req_user'],
        'date': request_data['date']
    }
    res = []
    cursor = get_db().cursor()
    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, istochnik, istochnik_del_status,
                       position, position_del_status FROM operations
                       JOIN users USING (user_id)
                       JOIN b_businesses USING (id_business)
                       JOIN b_istochniky USING (id_istochnik)
                       JOIN b_positions USING (id_position)
                       WHERE
                       data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                       ;""")

    prihod = cursor.fetchall()

    for i in prihod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type, business, business_del_status, cel, cely_del_status,
                           position, position_del_status FROM operations
                           JOIN users USING (user_id)
                           JOIN b_businesses USING (id_business)
                           JOIN b_cely USING (id_cel)
                           JOIN b_positions USING (id_position)
                           WHERE 
                           data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'
                           ;""")
    rashod = cursor.fetchall()

    for i in rashod:
        res.append(i)

    cursor.execute(
        f"""SELECT user_id, user_name, summa, data, operation_type FROM operations
                              JOIN users USING (user_id)
                              WHERE 
                              data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}' AND 
                              operation_type IN ('Собственные внесено', 'Собственные выведено')
                              """)
    sobstv = cursor.fetchall()

    for i in sobstv:
        res.append(i)

    cursor.execute(
        f"""SELECT perevody.user_get, perevody.summa, perevody.data, perevody.user_send, 
               users.user_name as user_send_name, users.user_del_status
               FROM perevody INNER JOIN users ON
               perevody.user_send = users.user_id
               WHERE data >= '{user_request['date'][0]}' AND data <= '{user_request['date'][1]}'""")
    perevody = cursor.fetchall()

    for i in perevody:
        cursor.execute(
            f"""SELECT user_name FROM users
                   WHERE user_id = '{i['user_get']}'""")
        name = cursor.fetchall()
        i.setdefault('user_get_name', name[0]['user_name'])


    polucheno = 0
    otpravleno = 0

    vneseno = 0
    vivideno = 0
    prihod = 0
    rashod = 0

    for i in perevody:
        if i['user_get'] == user_request['req_user']:
            polucheno += i['summa']
        elif i['user_send'] == user_request['req_user']:
            otpravleno += i['summa']

    for i in res:
        if i['operation_type'] == 'Приход' and i['user_id'] == user_request['req_user']:
            prihod += i['summa']
        elif i['operation_type'] == 'Расход' and i['user_id'] == user_request['req_user']:
            rashod += i['summa']

        elif i['operation_type'] == 'Собственные внесено' and i['user_id'] == user_request['req_user']:
            vneseno += i['summa']
        elif i['operation_type'] == 'Собственные выведено' and i['user_id'] == user_request['req_user']:
            vivideno += i['summa']




    grafic_perevod_get = []
    grafic_perevod_send = []
    grafic_operations = []

    for i in perevody:
        if i['user_get'] == user_request['id']:
            grafic_perevod_get.extend([{'summa': i['summa'], 'data': i['data']}])
        if i['user_send'] == user_request['id']:
            grafic_perevod_send.extend([{'summa': i['summa'], 'data': i['data']}])

    for i in res:
        if i['user_id'] == user_request['id']:
            grafic_operations.extend([{'summa': i['summa'], 'data': i['data'], 'operation_type': i['operation_type']}])

    return jsonify({'polucheno': polucheno, 'otpravleno': otpravleno, 'vneseno': vneseno,
                    'vivideno': vivideno, 'prihod': prihod, 'rashod': rashod,
                    'grafic': {'user_get': grafic_perevod_get,
                    'user_send': grafic_perevod_send,
                    'operations': grafic_operations},
                    'table': {'perevody': perevody,
                              'operations': res}})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5020)


