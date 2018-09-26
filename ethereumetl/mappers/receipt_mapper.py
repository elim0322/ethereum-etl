# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import pyarrow as pa

from ethereumetl.domain.receipt import EthReceipt
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.utils import hex_to_dec, to_normalized_address


class EthReceiptMapper(object):
    _fields = [pa.field('transaction_hash', pa.string()),
               pa.field('transaction_index', pa.int64()),
               pa.field('block_hash', pa.string()),
               pa.field('block_number', pa.int64()),
               pa.field('cumulative_gas_used', pa.int64()),
               pa.field('gas_used', pa.int64()),
               pa.field('contract_address', pa.string()),
               pa.field('root', pa.string()),
               pa.field('status', pa.int64())]

    _schema = pa.schema(_fields)

    def __init__(self, receipt_log_mapper=None):
        if receipt_log_mapper is None:
            self.receipt_log_mapper = EthReceiptLogMapper()
        else:
            self.receipt_log_mapper = receipt_log_mapper

    def json_dict_to_receipt(self, json_dict):
        receipt = EthReceipt()

        receipt.transaction_hash = json_dict.get('transactionHash', None)
        receipt.transaction_index = hex_to_dec(json_dict.get('transactionIndex', None))
        receipt.block_hash = json_dict.get('blockHash', None)
        receipt.block_number = hex_to_dec(json_dict.get('blockNumber', None))
        receipt.cumulative_gas_used = hex_to_dec(json_dict.get('cumulativeGasUsed', None))
        receipt.gas_used = hex_to_dec(json_dict.get('gasUsed', None))

        receipt.contract_address = to_normalized_address(json_dict.get('contractAddress', None))

        receipt.root = json_dict.get('root', None)
        receipt.status = hex_to_dec(json_dict.get('status', None))

        if 'logs' in json_dict:
            receipt.logs = [
                self.receipt_log_mapper.json_dict_to_receipt_log(log) for log in json_dict['logs']
            ]

        return receipt

    def receipt_to_dict(self, receipt):
        return {
            'type': 'receipt',
            'transaction_hash': receipt.transaction_hash,
            'transaction_index': receipt.transaction_index,
            'block_hash': receipt.block_hash,
            'block_number': receipt.block_number,
            'cumulative_gas_used': receipt.cumulative_gas_used,
            'gas_used': receipt.gas_used,
            'contract_address': receipt.contract_address,
            'root': receipt.root,
            'status': receipt.status
        }

    def receipts_to_dict(self, receipts):
        return {
            'type': 'receipt',
            'transaction_hash': [receipt.transaction_hash for receipt in receipts],
            'transaction_index': [receipt.transaction_index for receipt in receipts],
            'block_hash': [receipt.block_hash for receipt in receipts],
            'block_number': [receipt.block_number for receipt in receipts],
            'cumulative_gas_used': [receipt.cumulative_gas_used for receipt in receipts],
            'gas_used': [receipt.gas_used for receipt in receipts],
            'contract_address': [str(receipt.contract_address) for receipt in receipts],
            'root': [str(receipt.root) for receipt in receipts],
            'status': [receipt.status for receipt in receipts]
        }
