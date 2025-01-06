from bitcoinrpc.authproxy import AuthServiceProxy
import pandas as pd
import os
import glob
from datetime import datetime, timezone
import re
import hashlib
import base58
import subprocess
import threading
import time
import logging
import psycopg2
from Crypto.Hash import RIPEMD160

# Database connection setup
def initialize_connection():
    global connection, cursor
    connection = psycopg2.connect(
        host="localhost",
        database="DATABASE",
        user="postgres",
        password="PASS"
    )
    cursor = connection.cursor()
    

def save_to_postgresql(scheme, data):
    batch_size = 1000
    try:
        if scheme == "blocks":
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                cursor.executemany(insert_blocks, batch)
        elif scheme == "txns":
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                cursor.executemany(insert_txns, batch)
        elif scheme == "inputs":
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                cursor.executemany(insert_inputs, batch)
        elif scheme == "outputs":
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                cursor.executemany(insert_outputs, batch)
    except Exception as e:
        print(f"Error inserting data into {scheme}: {e}")
        raise

# Close the cursor and connection after all processing
def close_connection():
    if cursor:
        cursor.close()
    if connection:
        connection.close()

# SQL INSERT statement
insert_blocks = """
    INSERT INTO blockheader (
        indexnum, blockheight, blockhash,  
        time, timeutc, mediantime, epoch, epochsubsidy, blocksubsidy, fees, 
        transactionscount, vincount, vinvalue, voutcount, voutvalue, 
        strippedsize, size, weight, version, versionhex, merkleroot, 
        bits, chainwork, difficulty, nonce, coinbasescriptsig, sequence, 
        extranonce, extranoncereversed, extranoncedecimal
    ) VALUES (
        %(indexnum)s, %(blockheight)s, %(blockhash)s,
        %(time)s, %(timeutc)s, %(mediantime)s, %(epoch)s, %(epochsubsidy)s, %(blocksubsidy)s, %(fees)s, 
        %(transactionscount)s, %(vincount)s, %(vinvalue)s, %(voutcount)s, %(voutvalue)s, 
        %(strippedsize)s, %(size)s, %(weight)s, %(version)s, %(versionhex)s, %(merkleroot)s, 
        %(bits)s, %(chainwork)s, %(difficulty)s, %(nonce)s, %(coinbasescriptsig)s, %(sequence)s, 
        %(extranonce)s, %(extranoncereversed)s, %(extranoncedecimal)s
    )
"""
insert_txns = """
    INSERT INTO txnheader (
        indexnum, blockheight, transactionindex, transactionid, iscoinbase, coinbasemsg, 
        sequence, vincount, vinvalue, voutcount, voutvalue, fees, virtualsize_vb, weight_wu
    ) 
    VALUES (
        %(indexnum)s, %(blockheight)s, %(transactionindex)s, %(transactionid)s, %(iscoinbase)s, %(coinbasemsg)s, 
        %(sequence)s, %(vincount)s, %(vinvalue)s, %(voutcount)s, %(voutvalue)s, %(fees)s, %(virtualsize_vb)s, %(weight_wu)s
    )
"""

insert_inputs = """
    INSERT INTO inputs (
        indexnum, blockheight, transactionid, iscoinbase, vin, 
        origintransactionid, originvout, coinbasemsg, sequence, scriptsig, scriptwitness
    ) 
    VALUES (
        %(indexnum)s, %(blockheight)s, %(transactionid)s, %(iscoinbase)s, %(vin)s, 
        %(origintransactionid)s, %(originvout)s, %(coinbasemsg)s, %(sequence)s, %(scriptsig)s, %(scriptwitness)s
    )
"""

insert_outputs = """
    INSERT INTO outputs (
        indexnum, blockheight, transactionindex, transactionid, fromcoinbase, vout, address, 
        amount, scripttype, asm, descriptor, scripthex, isspent, spendingtxnid, spendingvin, 
        spendingblock, spendingscript, spendingwitness, isspendable
    ) 
    VALUES (
        %(indexnum)s, %(blockheight)s, %(transactionindex)s, %(transactionid)s, %(fromcoinbase)s, 
        %(vout)s, %(address)s, %(amount)s, %(scripttype)s, %(asm)s, %(descriptor)s, %(scripthex)s, 
        %(isspent)s, %(spendingtxnid)s, %(spendingvin)s, %(spendingblock)s, %(spendingscript)s, 
        %(spendingwitness)s, %(isspendable)s
    )
"""

def parse_scriptsig(scriptsig):
    index = 0
    result = []
    counter = 0
    while index <= len(scriptsig):
        counter += 1
        if counter == 3:
            break
        
        if index + 2 == len(scriptsig) or index == len(scriptsig):
            result.append((0, "00"))
            index = 100
            break
        
        # Read the length of the data group (first 2 hex characters)
        length = int(scriptsig[index:index + 2], 16)
        index += 2
        
        # Extract the next length * 2 hex characters (since each byte is 2 hex characters)
        data = scriptsig[index:index + length * 2]
       
        index += length * 2
        result.append((length, data))
    
    return result

def hex_to_decimal(hex_str):
    """Converts a hex string to its decimal representation, returning 0 on error."""
    try:
        return int(hex_str, 16)
    except ValueError:
        return 0
    
def reverse_little_endian(hex_str):
    """Reverses hex string in little-endian format, returning empty string on error."""
    try:
        return ''.join([hex_str[i:i+2] for i in range(0, len(hex_str), 2)][::-1])
    except IndexError:
        return "error"

    
def get_transaction_details(block_data, rpc_connection):
    currentHeight = block_data["height"]
    datetime_gmt0 = datetime.fromtimestamp(block_data["time"], timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    Index = datetime.fromtimestamp(block_data["time"], timezone.utc).strftime('%Y%m%d')
            
    transactions = block_data.get('tx', [])
    txn_index = 0
    block = []
    txns = []
    inputs = []
    outputs = []

    BlockVinCount = -1
    BlockVinValue = 0
    BlockVoutCount = 0
    BlockVoutValue = 0
    BlockFees = 0

    for tx_details in transactions:
        SumAmountVout = 0
        VinCount = len(tx_details["vin"])
        VinValue = 0
        VoutCount = len(tx_details["vout"])
        VoutValue = 0
        BlockVinCount += VinCount
        BlockVoutCount += VoutCount
                
        for vout in tx_details["vout"]:
            vout_n = vout.get("n", "")
            vout_addresses = vout["scriptPubKey"].get("address", "")
            vout_asm = vout["scriptPubKey"].get("asm", "")
            vout_desc = vout["scriptPubKey"].get("desc", "")
            vout_hex = vout["scriptPubKey"].get("hex", "")
            vout_type = vout["scriptPubKey"].get("type", "")
            SumAmountVout += vout.get("value", 0)
            vout_amount = int(vout.get("value", 0) * 100000000)
            VoutValue += vout_amount
            BlockVoutValue += vout_amount
            VinValue += vout_amount
            isspendable = True

            # Convert single address string to a list containing that string
            if isinstance(vout_addresses, str):
                vout_addresses = [vout_addresses]

            #print(vout_type)
            for vout_address in vout_addresses:
                if not vout_address:
                    if vout_type == "pubkey":
                        address = pubkey_to_pubkeyhash(extract_pubkey(vout_desc))
                    elif vout_type == "nonstandard":
                        address = "Unknown/Non-Standard"
                    elif vout_type == "multisig":
                        address = f"{decodehexscript(rpc_connection, vout_hex)} (DSMS)"
                    elif vout_type == "nulldata":
                        address = "OP_RETURN"
                        isspendable = False
                    else:
                        address = "Unknown"  # Assign a default value if type is unknown
                else:
                    address = vout_address
                #print(address)

            if vout_type == 'multisig':
                vout_type = extract_multisig_info(vout_asm)
            else:
                vout_type = vout_type

            outputs.append({
                "indexnum": int(Index),
                "blockheight": currentHeight,
                "transactionindex": txn_index,
                "transactionid": tx_details["txid"],
                "fromcoinbase": True if txn_index == 0 else False,
                "vout": vout_n,
                "address": address,
                "amount": vout_amount,
                "scripttype": vout_type,
                "asm": vout_asm,
                "descriptor": vout_desc,
                "scripthex": vout_hex,
                "isspent": False,
                "isspendable": isspendable,
                "spendingtxnid": None,
                "spendingvin": None,
                "spendingblock": None,
                "spendingscript": None,
                "spendingwitness": None
            })

        inputvin = -1    
        for vin in tx_details["vin"]:
            inputvin += 1
            if "coinbase" in vin:
                VinCount = 0
                VinValue = 0
                CoinbaseMessage = vin["coinbase"]   
                Sequence = vin["sequence"]   
                
                inputs.append({
                    "indexnum": int(Index),
                    "blockheight": currentHeight,
                    "transactionnum": txn_index,
                    "transactionid": tx_details["txid"],
                    "iscoinbase": True if txn_index == 0 else False,
                    "vin": int(inputvin),
                    "origintransactionid": "-",
                    "originvout": None,
                    "coinbasemsg": CoinbaseMessage,  
                    "sequence": Sequence,  
                    "scriptsig": None,
                    "scriptwitness": None
                })
            else:
                witness = vin.get("txinwitness", [])
                witness_str = ", ".join([item if item else "<empty>" for item in witness]) if witness else ""  

                inputs.append({
                    "indexnum": int(Index),
                    "blockheight": currentHeight,
                    "transactionnum": txn_index,
                    "transactionid": tx_details["txid"],
                    "iscoinbase": True if txn_index == 0 else False,
                    "vin": int(inputvin),
                    "origintransactionid": vin["txid"],
                    "originvout": vin["vout"],
                    "coinbasemsg": "",
                    "sequence": vin["sequence"],  
                    "scriptsig": vin["scriptSig"]["hex"],
                    "scriptwitness": witness_str
                })

        tx_fee = tx_details.get("fee", 0)
        fee_satoshis = int(tx_fee * 100000000)
        VinValue += fee_satoshis
        
        BlockVinValue += VinValue
        BlockFees += fee_satoshis
            
        txns.append({
            "indexnum": int(Index),
            "blockheight": currentHeight,
            "transactionindex": txn_index,
            "transactionid": tx_details["txid"],
            "iscoinbase": True if txn_index == 0 else False,
            "coinbasemsg": CoinbaseMessage if txn_index == 0 else None,  
            "sequence": str(Sequence),  
            "vincount": VinCount,
            "vinvalue": int(VinValue),
            "voutcount": VoutCount,
            "voutvalue": int(VoutValue),
            "fees": fee_satoshis,
            "virtualsize_vb": tx_details["vsize"],
            "weight_wu": tx_details["weight"],
        })

        txn_index += 1

    parsed_data = parse_scriptsig(CoinbaseMessage)

    for length, data in parsed_data:
        extranonceHex = str(data)

    for length, data in parsed_data:
        extranonceHexReversed = str(reverse_little_endian(data))
        extranonceHexReversedDec = hex_to_decimal(extranonceHexReversed)

    if currentHeight == 0:
        previousblockhash = ""
    else:
        previousblockhash = block_data["previousblockhash"]

    if 0 <= currentHeight < 210000:
        epoch = 1
        subsidy = 50* 100000000 
    elif 210000 <= currentHeight < 420000:
        epoch = 2
        subsidy = 25* 100000000
    elif 420000 <= currentHeight < 630000:
        epoch = 3
        subsidy = 12.5* 100000000
    elif 630000 <= currentHeight < 840000:
        epoch = 4
        subsidy = 6.25* 100000000
    else:
        epoch = 5
        subsidy = 3.125* 100000000
    blocksubsidy = int(BlockVoutValue) - int(BlockVinValue)
    
    block.append({
        "indexnum": int(Index),
        "blockheight": block_data["height"],
        "blockhash": block_data["hash"],
        "previousblockhash": previousblockhash,
        "nextblockhash": block_data["nextblockhash"],
        "version": block_data["version"],
        "versionhex": block_data["versionHex"],
        "merkleroot": str(block_data["merkleroot"]),
        "time": block_data["time"],
        "timeutc": datetime_gmt0,
        "mediantime": block_data["mediantime"],
        "epoch": epoch,
        "epochsubsidy": subsidy,
        "blocksubsidy": blocksubsidy,
        "transactionscount": block_data["nTx"],
        "vincount": BlockVinCount,
        "vinvalue": int(BlockVinValue),
        "voutcount": BlockVoutCount,
        "voutvalue": int(BlockVoutValue),
        "fees": BlockFees,
        "nonce": str(block_data["nonce"]),
        "coinbasescriptsig": CoinbaseMessage,
        "sequence": str(Sequence),
        "extranonce": extranonceHex,
        "extranoncereversed": extranonceHexReversed,
        "extranoncedecimal": extranonceHexReversedDec,
        "bits": str(block_data["bits"]),
        "difficulty": str(block_data["difficulty"]),
        "chainwork": block_data["chainwork"],
        "strippedsize": block_data["strippedsize"],
        "size": block_data["size"],
        "weight": block_data["weight"]
    })

    nextblock = int(block_data["height"]) + 1
    nexthash = block_data["nextblockhash"]
    return block, txns, inputs, outputs, Index, datetime_gmt0

def extract_pubkey(vout_desc):
    pk_pattern = r"pk\((.*?)\)"
    raw_pattern = r"raw\((.*?)\)"
    pk_match = re.search(pk_pattern, vout_desc)
    raw_match = re.search(raw_pattern, vout_desc)
    
    if pk_match:
        return pk_match.group(1)
    elif raw_match:
        return raw_match.group(1)
    else:
        return None

def extract_multisig_info(ASM):
    parts = ASM.split()
    
    if len(parts) >= 2:
        m = parts[0]  # Threshold "m" is the first number
        n_index = parts.index("OP_CHECKMULTISIG") - 1  # Find the index before "OP_CHECKMULTISIG"
        n = parts[n_index] if n_index >= 0 else "Invalid multisig parameters"  # Total number of keys "n"
        return f"Multisig {m}/{n}"
    else:
        return "Multisig Error"


def pubkey_to_pubkeyhash(pubkey):
    pubkey_bytes = bytes.fromhex(pubkey)
    sha256_hash = hashlib.sha256(pubkey_bytes).digest()
    ripemd160_hash = RIPEMD160.new(sha256_hash).digest()
    pubkeyhash_with_network_byte = b'\x00' + ripemd160_hash
    checksum = hashlib.sha256(hashlib.sha256(pubkeyhash_with_network_byte).digest()).digest()[:4]
    pubkeyhash_with_checksum = pubkeyhash_with_network_byte + checksum
    address = base58.b58encode(pubkeyhash_with_checksum)
    return address.decode('utf-8')

def decodehexscript(rpc_connection, vout_hex):
    decodedhex = rpc_connection.decodescript(vout_hex)
    address = decodedhex.get("p2sh")  
#    print(address)
    return address

def get_height(connection, startingHeight):
    try:
        cursor = connection.cursor()

        cursor.execute("SELECT COUNT(*) FROM blockheader;")
        row_count = cursor.fetchone()[0]

        if row_count == 0:
            currentHeight = startingHeight
            print(f"Blockheader table is empty, using startingHeight: {currentHeight}")
        else:
            cursor.execute("SELECT MAX(blockheight) FROM blockheader;")
            largest_amount = cursor.fetchone()[0]
            
            if largest_amount is None:
                print("No valid block found in blockheader.")
                currentHeight = startingHeight
            else:
                print(f"Largest block in blockheader: {largest_amount}")
                currentHeight = largest_amount + 1
        cursor.close()
        return currentHeight
    
    except Exception as e:
        print(f"Error: {e}")
        return None

def export_data(blocks_data, txns_data, inputs_data, outputs_data, currentHeight):
    try:
        # Assuming `blocks_data` is a list of block dictionaries
        save_to_postgresql("blocks", blocks_data)
        save_to_postgresql("txns", txns_data)
        save_to_postgresql("inputs", inputs_data)
        save_to_postgresql("outputs", outputs_data)
        connection.commit()
        
    except Exception as e:
        print(f"Error processing block {currentHeight}: {e}")
        connection.rollback()  
        main() 
    
    try:           
        #print(" Processing UTXOs Set.")
        utxoupdated = update_utxoset(currentHeight)
        connection.commit()
    
        return utxoupdated
        
    except Exception as e:
        print(f"Error processing block {currentHeight}: {e}")
        connection.rollback() 
        main() 

def update_utxoset(maxheight):   
    # SQL query to perform a batched join and update in the outputs table
    update_query = f"""
    UPDATE outputs
    SET
        isspent = True,
        spendingtxnid = inputs.transactionid,
        spendingvin = inputs.vin,
        spendingblock = inputs.blockheight,
        spendingscript = inputs.scriptsig,
        spendingwitness = inputs.scriptwitness
    FROM inputs
    WHERE
        outputs.isspent = False
        AND inputs.origintransactionid = outputs.transactionid
        AND inputs.originvout = outputs.vout;
    """

    cursor.execute(update_query)
    utxoupdated = cursor.rowcount  

    clear_inputs_query = "DELETE FROM inputs;"
    cursor.execute(clear_inputs_query)

    return utxoupdated

def main():
    rpc_user = "USER"
    rpc_password = "PASS"
    rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_password}@localhost:8332")

    maxHeight = int(rpc_connection.getblockchaininfo().get('blocks')) 
    startingHeight = 0

    initialize_connection()
    currentHeight = get_height(connection, startingHeight)
    
    starttime = datetime.now()
    print(f"Start Time: {starttime}\n")

    first_block = currentHeight
    transaction_count = 0
    
    blocks_data = []
    txns_data = []
    inputs_data = []
    outputs_data = []
                
    while currentHeight <= maxHeight:
        try:
            block_hash = rpc_connection.getblockhash(currentHeight)
            block_data = rpc_connection.getblock(block_hash, 2)
            
            if block_data:
               block, txns, inputs, outputs, Index, datetime_gmt0 = get_transaction_details(block_data, rpc_connection)
               if block is not None and txns is not None and inputs is not None and outputs is not None:
                    blocks_data.extend(block)
                    txns_data.extend(txns)
                    inputs_data.extend(inputs)
                    outputs_data.extend(outputs)
                    transaction_count += len(block_data.get('tx', []))

            if currentHeight % 1000 == 0:
                if blocks_data and txns_data and inputs_data and outputs_data:    
                    export_data(blocks_data, txns_data, inputs_data, outputs_data, currentHeight)
                    
                    endtime = datetime.now()
                    duration = endtime - starttime
                    total_seconds = duration.total_seconds()
                    duration_minutes = int(total_seconds // 60)
                    duration_seconds = int(total_seconds % 60)

                    blocks_data = []
                    txns_data = []
                    inputs_data = []
                    outputs_data = []                                   
                    print(f" Block Height: {first_block:06d} - {currentHeight:06d} | Txns: {transaction_count:,} | Processing Time: {duration_minutes:02d}:{duration_seconds:02d}")

                    first_block = currentHeight + 1
                    starttime = datetime.now()
                    transaction_count = 0
                
            if currentHeight == maxHeight:
                if blocks_data and txns_data and inputs_data and outputs_data:    
                    export_data(blocks_data, txns_data, inputs_data, outputs_data, currentHeight)

                    endtime = datetime.now()
                    duration = endtime - starttime
                    total_seconds = duration.total_seconds()
                    duration_minutes = int(total_seconds // 60)
                    duration_seconds = int(total_seconds % 60)
                    print(f" Block Height: {first_block:06d} - {currentHeight:06d} | Txns: {transaction_count:,} | Processing Time: {duration_minutes:02d}:{duration_seconds:02d}")

                break

            else:
                currentHeight = currentHeight + 1
            
        except Exception as e:
            print(f"Main - Error processing block {currentHeight}: {e}")
            run_with_interval()

    try:
        maxHeight = int(rpc_connection.getblockchaininfo().get('blocks'))
        if currentHeight >= maxHeight:
            print("Process Completed, sleeping 30 seconds")

    except Exception as e:
        close_connection()   
        run_with_interval()

def run_with_interval():
    while True:
        main()
        print("Waiting for 300 seconds before restarting...")
        time.sleep(300)

if __name__ == "__main__":
    run_with_interval()
