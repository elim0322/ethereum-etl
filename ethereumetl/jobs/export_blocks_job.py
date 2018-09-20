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


import json

from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.jobs.base_job import BaseJob
from ethereumetl.json_rpc_requests import generate_get_block_by_number_json_rpc
from ethereumetl.mappers.block_mapper import EthBlockMapper
from ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from ethereumetl.utils import rpc_response_batch_to_results, validate_range


# Exports blocks and transactions
class ExportBlocksJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter,
            export_blocks=True,
            export_transactions=True):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.batch_web3_provider = batch_web3_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.export_blocks = export_blocks
        self.export_transactions = export_transactions
        if not self.export_blocks and not self.export_transactions:
            raise ValueError('At least one of export_blocks or export_transactions must be True')

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        blocks_rpc = list(generate_get_block_by_number_json_rpc(block_number_batch, self.export_transactions))
        response = self.batch_web3_provider.make_request(json.dumps(blocks_rpc))
        results = rpc_response_batch_to_results(response)
        blocks = [self.block_mapper.json_dict_to_block(result) for result in results]

        for block in blocks:
            self._export_block(block)

    def _export_block(self, block):
        if self.export_blocks:
            self.item_exporter.export_item(self.block_mapper.block_to_dict(block))
        if self.export_transactions:
            for tx in block.transactions:
                self.item_exporter.export_item(self.transaction_mapper.transaction_to_dict(tx))

    def _export_as_parquet(self, block):
        """This method exports blocks as column-oriented Parquet format, as
        opposed to the row-oriented csv format. In doing so, the attributes of
        each block object saved in memory as lists, which then make up pyarrow
        Table objects.
        """

        # initialize list containers
        number            = []
        hash              = []
        parent_hash       = []
        nonce             = []
        sha3_uncles       = []
        logs_bloom        = []
        transactions_root = []
        state_root        = []
        receipts_root     = []
        miner             = []
        difficulty        = []
        total_difficulty  = []
        size              = []
        extra_data        = []
        gas_limit         = []
        gas_used          = []
        timestamp         = []
        transaction_count = []

        # unpack block object & append to the containers
        for each in block:
            number.append(each.number)
            hash.append(each.hash)
            parent_hash.append(each.parent_hash)
            nonce.append(each.nonce)
            sha3_uncles.append(each.sha3_uncles)
            logs_bloom.append(each.logs_bloom)
            transactions_root.append(each.transactions_root)
            state_root.append(each.state_root)
            receipts_root.append(each.receipts_root)
            miner.append(each.miner)
            difficulty.append(str(each.difficulty))             #cast as str
            total_difficulty.append(str(each.total_difficulty)) #cast as str
            size.append(each.size)
            extra_data.append(each.extra_data)
            gas_limit.append(each.gas_limit)
            gas_used.append(each.gas_used)
            timestamp.append(each.timestamp)
            transaction_count.append(each.transaction_count)

        # convert list to Arrow array object
        import pyarrow as pa
        arr_number            = pa.array(number,            type="int64")
        arr_hash              = pa.array(hash,              type="str")
        arr_parent_hash       = pa.array(parent_hash,       type="str")
        arr_nonce             = pa.array(nonce,             type="str")
        arr_sha3_uncles       = pa.array(sha3_uncles,       type="str")
        arr_logs_bloom        = pa.array(logs_bloom,        type="str")
        arr_transactions_root = pa.array(transactions_root, type="str")
        arr_state_root        = pa.array(state_root,        type="str")
        arr_receipts_root     = pa.array(receipts_root,     type="str")
        arr_miner             = pa.array(miner,             type="str")
        arr_difficulty        = pa.array(difficulty,        type="str")
        arr_total_difficulty  = pa.array(total_difficulty,  type="str")
        arr_size              = pa.array(size,              type="int32")
        arr_extra_data        = pa.array(extra_data,        type="str")
        arr_gas_limit         = pa.array(gas_limit,         type="int64")
        arr_gas_used          = pa.array(gas_used,          type="int64")
        arr_timestamp         = pa.array(timestamp,         type="int64")
        arr_transaction_count = pa.array(transaction_count, type="int16")

        # create Arrow table object
        table = pa.Table.from_arrays(
            arrays = [arr_number,arr_hash,arr_parent_hash,arr_nonce,
                      arr_sha3_uncles,arr_logs_bloom,arr_transactions_root,
                      arr_state_root,arr_receipts_root,arr_miner,arr_difficulty,
                      arr_total_difficulty,arr_size,arr_extra_data,arr_gas_limit,
                      arr_gas_used,arr_timestamp,arr_transaction_count],
            names = ['number','hash','parent_hash','nonce','sha3_uncles',
                     'logs_bloom','transactions_root','state_root',
                     'receipts_root','miner','difficulty','total_difficulty',
                     'size','extra_data','gas_limit','gas_used','timestamp',
                     'transaction_count']
        )

        # save as parquet
        pa.parquet.write_table()

        return

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
