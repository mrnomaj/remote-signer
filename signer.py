#!/usr/bin/env python3

#########################################################
# Written by Carl Youngblood, carl@blockscale.net
# Copyright (c) 2018 Blockscale LLC
# released under the MIT license
#########################################################

from flask import Flask, request, Response, json, jsonify
from src.remote_signer import RemoteSigner
from src.system_db import SystemDB
from os import path
import logging

logging.basicConfig(filename='./remote-signer.log', format='%(asctime)s %(message)s', level=logging.INFO)

MIN_ESCROW_TIME = 3600 * 24 # 1 day
MAX_ESCROW_TIME = 3600 * 24 * 7 # 7 days

sysDB = SystemDB('system.db', MIN_ESCROW_TIME, MAX_ESCROW_TIME)
#sysDB.provision_database()

app = Flask(__name__)

# sample config used for testing
config = {
    'hsm_username': 'resigner',
    'hsm_slot': 1,
    'hsm_lib': '/opt/cloudhsm/lib/libcloudhsm_pkcs11.so',
    'node_addr': 'http://node.internal:8732',
    'keys': {
        'tz3aTaJ3d7Rh4yXpereo4yBm21xrs4bnzQvW': {
            'public_key': 'p2pk67jx4rEadFpbHdiPhsKxZ4KCoczLWqsEpNarWZ7WQ1SqKMf7JsS',
            'private_handle': 7,
            'public_handle': 9
        }
    }
}

logging.info('Opening keys.json')
if path.isfile('keys.json'):
    logging.info('Found keys.json')
    with open('keys.json', 'r') as myfile:
        json_blob = myfile.read().replace('\n', '')
        logging.info('Parsed keys.json successfully as JSON')
        config = json.loads(json_blob)
        logging.info('Config contains: {}'.format(json.dumps(config, indent=2)))


@app.route('/keys/<key_hash>', methods=['POST'])
def sign(key_hash):
    response = None
    try:
        data = request.get_json(force=True)
        if key_hash in config['keys']:
            logging.info('Found key_hash {} in config'.format(key_hash))
            key = config['keys'][key_hash]
            logging.info('Attempting to sign {}'.format(data))
            rs = RemoteSigner(sysDB, config, data)
            response = jsonify({
                'signature': rs.sign(key['private_handle'])
            })
            logging.info('Response is {}'.format(response))
        else:
            logging.warning("Couldn't find key {}".format(key_hash))
            response = Response('Key not found', status=404)
    except Exception as e:
        data = {'error': str(e)}
        logging.error('Exception thrown during request: {}'.format(str(e)))
        response = app.response_class(
            response=json.dumps(data),
            status=500,
            mimetype='application/json'
        )
    logging.info('Returning flask response {}'.format(response))
    return response


@app.route('/keys/<key_hash>', methods=['GET'])
def get_public_key(key_hash):
    response = None
    try:
        if key_hash in config['keys']:
            key = config['keys'][key_hash]
            response = jsonify({
                'public_key': key['public_key']
            })
            logging.info('Found public key {} for key hash {}'.format(key['public_key'], key_hash))
        else:
            logging.warning("Couldn't public key for key hash {}".format(key_hash))
            response = Response('Key not found', status=404)
    except Exception as e:
        data = {'error': str(e)}
        logging.error('Exception thrown during request: {}'.format(str(e)))
        response = app.response_class(
            response=json.dumps(data),
            status=500,
            mimetype='application/json'
        )
    logging.info('Returning flask response {}'.format(response))
    return response


@app.route('/authorized_keys', methods=['GET'])
def authorized_keys():
    return app.response_class(
        response=json.dumps({}),
        status=200,
        mimetype='application/json'
    )

'''
GET /transfers/?id=<someid>: returns all requests that are pending if id is empty, or all requests that match id in JSON format
POST /transfers/create: {"type":"transfer","amt":"float amount in tez","src":"src addr","dest":"dest addr","fee":"fee to pay baker"} - returns 200 OK or Bad Request
POST /transfers/delete: {"id":"id of request returned from POST /transfers/create"} - 200 OK or Bad Request
'''
@app.route('/transfers/<id>', methods=['GET'])
def get_transfers_with_id(id):
    return jsonify(sysDB.op_select(id))

@app.route('/transfers', methods=['GET'])
def get_transfers():
    return jsonify(sysDB.op_select())

@app.route('/transfers', methods=['POST'])
def insert_transfer():
    data = request.get_json(force=True)
    return jsonify(sysDB.op_insert(data))

@app.route('/transfers/delete/<id>', methods=['POST'])
def delete_transfer(id):
    return jsonify(sysDB.op_delete(id))

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
