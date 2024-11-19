from ic.identity import Identity
from ic.client import Client
from ic.agent import Agent
from ic.candid import encode, Types
from datetime import datetime


def get_account_tx(account,query_amount):

    types = Types.Record({
            'max_results': Types.Nat,
            'start': Types.Opt(Types.Nat),
            'account':Types.Record({'owner':Types.Principal,'subaccount':Types.Opt(Types.Vec(Types.Nat8))}),})
    values = {
            'max_results': query_amount,
            'start':[] ,
            'account':{'owner':account,'subaccount':[]},
    }
    params = [{'type': types, 'value': values}]
    return  encode(params)
    
def query_transactions(agent, like_index, usr_account, query_amount, cutoff_date):
    
    while True:
        # トランザクションを取得
        usr_tx = agent.query_raw(
            like_index, 
            "get_account_transactions", 
            get_account_tx(usr_account, query_amount)
        )
        # データ整形
        processed_data = process_transactions(usr_tx)

        
        # トランザクション件数が query_amount と同じ場合、cutoff_date 以降のトランザクションが無いか確認
        if len(processed_data) == query_amount:
            # cutoff_date 以降のトランザクションが存在する場合、query_amount を増やして再試行
            if any(tx['Timestamp'] >= cutoff_date for tx in processed_data):
                query_amount += query_amount
                print(f"Querying more transactions, new query amount: {query_amount}")
            else:
                # cutoff_date 以降のトランザクションが無い場合は処理を終了
                print("No transactions after cutoff_date. Process complete.")
                # cutoff_date より新しいトランザクションの数を取得
                new_transactions_count = sum(1 for tx in processed_data if tx['Timestamp'] >= cutoff_date)
                
                print(new_transactions_count)
                return new_transactions_count 
                # break
        else:
            # トランザクション件数が query_amount 未満の場合は処理を終了
            print("Transaction count is less than query_amount. Process complete.")
            new_transactions_count = sum(1 for tx in processed_data if tx['Timestamp'] >= cutoff_date)
            return new_transactions_count
            # break


def process_transactions(data):
    transactions = []
    
    for item in data:
        transaction_type = item.get('type')
        value = item.get('value', {})
        
        for key, details in value.items():
            # タイムスタンプ（正しいフィールドから取得）
            records = details.get('_3331539157', [])
            for record in records:
                record_id = record.get('_23515')
                record_details = record.get('_1266835934', {})
                
                # タイムスタンプを `_2781795542` から取得
                timestamp = record_details.get('_2781795542')  # 正しいタイムスタンプフィールド
                
                operation = record_details.get('_1191829844')
                amount = record_details.get('_3664621355', [{}])[0].get('_3573748184', 0)
                
                sender = (
                    record_details.get('_3664621355', [{}])[0]
                    .get('_25979', {})
                    .get('_947296307', None)
                )
                receiver = (
                    record_details.get('_3664621355', [{}])[0]
                    .get('_1136829802', {})
                    .get('_947296307', None)
                )
                
                # データをリストに追加
                transactions.append({
                    'Transaction ID': record_id,
                    'Type': operation,
                    'Sender': sender,
                    'Receiver': receiver,
                    'Amount': amount,
                    'Timestamp': timestamp  # 修正済みタイムスタンプ
                })
    
    return transactions
                
if __name__ == "__main__": 
    
    # anonimous agent
    ano = Identity()
    client = Client(url = "https://icp-api.io")
    agent = Agent(ano, client)
    
    #token_index_canistar
    like_index = "mvtuy-wiaaa-aaaam-adh7a-cai"
    
    usr_account = ["vb4hy-pfb2e-ka2ci-xz6k3-rpuap-i3cye-6fzs5-hkvhy-4op2e-aflph-6ae"]
    
    # "hgqso-fkdrc-krtb4-qecaf-dcwux-ipzxz-5v7rd-lwu5o-knudt-2zn77-mae" 運営のpid容量大きく上手く処理できなかった
    #queryの容量制限により恐らく全件は取得できない2000件までだと思われる。
    # 'start': Types.Opt(Types.Nat)で範囲を指定できるが全txの指定したidから遡る仕様のため使いずらい
    query_amount = 100
    
    # タイムスタンプしきい値を設定　datetime(年, 月, 日, 時, 分, 秒)
    cutoff_date = datetime(2024, 11, 9, 0, 0, 0).timestamp()
    
    for _ in usr_account:
        # data整形
        tx_amount = query_transactions(agent, like_index, _, query_amount, cutoff_date)
        print(_,tx_amount)
