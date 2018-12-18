#########################################################
# Written by Carl Youngblood, carl@blockscale.net
# Copyright (c) 2018 Blockscale LLC
# released under the MIT license
#########################################################

import struct 
import string
from src.dynamodb_client import DynamoDBClient
from pyhsm.hsmclient import HsmClient, HsmAttribute
from pyhsm.hsmenums import HsmMech
from pyhsm.convert import hex_to_bytes, bytes_to_hex
from binascii import unhexlify
from os import environ
import bitcoin
from pyblake2 import blake2b
import logging
import uuid
from dyndbmutex.dyndbmutex import DynamoDbMutex

'''
after POST /transfers/create success: send email to dest address indicating an operation of type TYPE was authorized
for amount AMT from SRC to DEST with fee FEE after an escrow period of MIN_ESCROW_TIME

on incoming signature request with revelation or transfer preamble:
1. check if request with same fee, origin, destination exists in table with MIN_ESCROW_TIME interval
2. if request exists, remove row and sign operation
3. if request does not fail, do not sign operation

routes:
    `
'''

class RemoteSigner:
    systemDB = None
    BLOCK_PREAMBLE = 1
    ENDORSEMENT_PREAMBLE = 2
    TRANSFER_PREAMBLE = 3
    REVELATION_PREAMBLE = 4 # TODO: verify this
    TEST_SIGNATURE = 'p2sigfqcE4b3NZwfmcoePgdFCvDgvUNa6DBp9h7SZ7wUE92cG3hQC76gfvistHBkFidj1Ymsi1ZcrNHrpEjPXQoQybAv6rRxke'
    P256_SIGNATURE = struct.unpack('>L', b'\x36\xF0\x2C\x34')[0]  # results in p2sig prefix when encoded with base58
    ED25519_SIGNATURE = 42778592786 # results in edsig prefix when encoded with base58
    KEYTYPE = ED25519_SIGNATURE # change this to P256_SIGNATURE for p256 support

    def __init__(self, sysDB, config, payload='', rpc_stub=None):
        self.keys = config['keys']
        self.payload = payload
        logging.info('Verifying payload')
        self.data = self.decode_block(self.payload)
        logging.info('Payload {} is valid'.format(self.data))
        self.rpc_stub = rpc_stub
        self.hsm_slot = config['hsm_slot']
        hsm_user = config['hsm_username']
        logging.info('HSM user is {}'.format(config['hsm_username']))
        logging.info('Attempting to read env var HSM_PASSWORD')
        hsm_password = environ['HSM_PASSWORD']
        self.hsm_pin = '{}:{}'.format(hsm_user, hsm_password)
        self.hsm_libfile = config['hsm_lib']
        logging.info('HSM lib is {}'.format(config['hsm_lib']))
        self.node_addr = config['node_addr']
        # Add support for DynamoDB on 9-22-18 by LY
        self.ddb_region = environ['REGION']
        self.ddb_table = environ['DDB_TABLE']
        self.systemDB = sysDB

    @staticmethod
    def valid_block_format(blockdata):
        return all(c in string.hexdigits for c in blockdata)

    @staticmethod
    def decode_block(data):
        return RemoteSigner.valid_block_format(data) and bytes.fromhex(data)

    def is_block(self):
        return self.data and list(self.data)[0] == self.BLOCK_PREAMBLE

    def is_endorsement(self):
        return list(self.data)[0] == self.ENDORSEMENT_PREAMBLE

    def is_revelation(self):
        return self.data and list(self.data)[0] == self.REVELATION_PREAMBLE

    def is_transfer(self):
        return self.data and list(self.data)[0] == self.TRANSFER_PREAMBLE

    def get_block_level(self):
        level = -1
        if self.is_endorsement():
            hex_level = self.payload[-8:]
        else:
            hex_level = self.payload[10:18]
        level = struct.unpack('>L', unhexlify(hex_level))[0]
        logging.info('Block level is {}'.format(level))
        return level

    def not_already_signed(self):
        payload_level = self.get_block_level()
        if self.is_block():
            sig_type = 'Baking'
        elif self.is_endorsement():
            sig_type = 'Endorsement'
        elif self.is_transfer():
            sig_type = 'Transfer'
        else:
            sig_type = 'Revelation'
        ddb = DynamoDBClient(self.ddb_region, self.ddb_table, sig_type, payload_level)
        not_signed = ddb.check_double_signature()
        if not_signed:
            logging.info('{} signature for level {} has not been generated before'.format(sig_type, payload_level))
        else:
            logging.error('{} signature for level {} has already been generated!'.format(sig_type, payload_level))
        return not_signed

    @staticmethod
    def b58encode_signature(sig, scheme=P256_SIGNATURE):
        return bitcoin.bin_to_b58check(sig, magicbyte=scheme)

    def sign(self, handle, test_mode=False):
        # This code acquires a mutex lock using https://github.com/chiradeep/dyndb-mutex
        # generate a unique name for this process/thread
        print(self.payload)
        print('first 67: {}'.format(self.payload[:67]))
        print(struct.unpack('>L', unhexlify(self.payload[11:19]))[0])
        ddb_region = environ['REGION']
        my_name = str(uuid.uuid4()).split("-")[0]
        if self.is_block():
            sig_type = 'Baking'
        elif self.is_endorsement():
            sig_type = 'Endorsement'
        elif self.is_transfer():
            sig_type = 'Transfer'
        else:
            sig_type = 'Revelation'
        m = DynamoDbMutex(sig_type, holder=my_name, timeoutms=60 * 1000, region_name=ddb_region)
        locked = m.lock() # attempt to acquire the lock
        if locked:
            encoded_sig = ''
            data_to_sign = self.payload
            logging.info('About to sign {} with key handle {}'.format(data_to_sign, handle))
            if self.valid_block_format(data_to_sign):
                logging.info('Block format is valid')
                if self.is_block() or self.is_endorsement() or self.is_transfer() or self.is_revelation():
                    logging.info('Preamble is valid')
                    if self.not_already_signed():
                        if test_mode:
                            return self.TEST_SIGNATURE
                        else:
                            logging.info('About to sign with HSM client. Slot = {}, lib = {}, handle = {}'.format(self.hsm_slot, self.hsm_libfile, handle))
                            with HsmClient(slot=self.hsm_slot, pin=self.hsm_pin, pkcs11_lib=self.hsm_libfile) as c:
                                hashed_data = blake2b(hex_to_bytes(data_to_sign), digest_size=32).digest()
                                logging.info('Hashed data to sign: {}'.format(hashed_data))
                                sig = c.sign(handle=handle, data=hashed_data, mechanism=HsmMech.ECDSA)
                                logging.info('Raw signature: {}'.format(sig))
                                encoded_sig = RemoteSigner.b58encode_signature(sig, self.KEYTYPE)
                                logging.info('Base58-encoded signature: {}'.format(encoded_sig))
                    else:
                        logging.error('Invalid level')
                        m.release() # release the lock
                        raise Exception('Invalid level')
                else:
                    logging.error('Invalid preamble')
                    m.release() # release the lock
                    raise Exception('Invalid preamble')
            else:
                logging.error('Invalid payload')
                m.release() # release the lock
                raise Exception('Invalid payload')
            m.release() # release the lock
            return encoded_sig
        else: # lock could not be acquired
            logging.error('Could not acquire lock')
            raise Exception('Could not acquire lock')
