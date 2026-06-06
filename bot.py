import websocket
import json
import requests
import threading
import time

TOKEN = "user_token"
OWNER_ID = 1234567899876543 # ---------- PUT YOUR USER ID HERE   ----------
PREFIX = "!"
HEADERS = {"Authorization": TOKEN, "Content-Type": "application/json"}
kicking = False
ws_app = None

def send_message(channel_id, content):
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    requests.post(url, headers=HEADERS, json={"content": content})

def delete_message(channel_id, message_id):
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}"
    requests.delete(url, headers=HEADERS)

# ---------- Gateway Member Fetch ----------
members_chunks = []
chunks_received = 0
chunk_lock = threading.Lock()
chunk_done_event = threading.Event()

def request_members(guild_id):
    """ Request all members for a guild via Gateway (user account compatible) """
    if ws_app:
        ws_app.send(json.dumps({
            "op": 8,
            "d": {
                "guild_id": str(guild_id),
                "query": "",
                "limit": 0
            }
        }))

def on_guild_members_chunk(data):
    global members_chunks, chunks_received
    chunk = data.get("members", [])
    chunk_index = data.get("chunk_index", 0)
    chunk_count = data.get("chunk_count", 1)
    with chunk_lock:
        members_chunks.extend(chunk)
        chunks_received += 1
        print(f"[CHUNK] {chunks_received}/{chunk_count} (got {len(chunk)} members)")
        if chunks_received >= chunk_count:
            chunk_done_event.set()

# ---------- KickAll Function ----------
def kick_all(guild_id, channel_id):
    global kicking, members_chunks, chunks_received, chunk_done_event
    kicking = True
    members_chunks = []
    chunks_received = 0
    chunk_done_event.clear()

    send_message(channel_id, f"📋 Requesting member list for server {guild_id}...")
    request_members(guild_id)

    # Wait up to 60 seconds for chunks (bade server ke liye)
    if not chunk_done_event.wait(timeout=60):
        send_message(channel_id, "⚠️ Timed out. Proceeding with received members...")

    members = members_chunks
    print(f"[DEBUG] Total members fetched: {len(members)}")
    targets = [(m['user']['id'], m['user']['username']) for m in members if m['user']['id'] != str(OWNER_ID)]
    total = len(targets)
    send_message(channel_id, f"🎯 Targets: **{total}**")

    if total == 0:
        send_message(channel_id, "❌ No members to kick!")
        kicking = False
        return

    kicked = 0
    failed = 0
    rate_limits = 0
    lock = threading.Lock()
    start_time = time.time()

    def kick_user(uid, name):
        nonlocal kicked, failed, rate_limits
        while True:
            r = requests.delete(f"https://discord.com/api/v10/guilds/{guild_id}/members/{uid}", headers=HEADERS)
            if r.status_code == 204:
                with lock: kicked += 1
                print(f"👢 {name}")
                return
            elif r.status_code == 429:
                retry = r.json().get('retry_after', 5)
                with lock: rate_limits += 1
                print(f"⏳ RL {retry}s")
                time.sleep(retry + 0.5)
                continue
            elif r.status_code == 403:
                with lock: failed += 1
                print(f"⛔ No perm: {name}")
                return
            else:
                with lock: failed += 1
                print(f"❌ {name} ({r.status_code})")
                return

    batch_size = 5
    for i in range(0, total, batch_size):
        batch = targets[i:i+batch_size]
        threads = []
        for uid, name in batch:
            t = threading.Thread(target=kick_user, args=(uid, name))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        time.sleep(0.1)

    total_time = time.time() - start_time
    send_message(channel_id, f"✅ Kicked: {kicked} | ❌ Failed: {failed} | ⏱️ {total_time:.1f}s")
    kicking = False

# ---------- WebSocket Events ----------
def on_message(ws, message):
    global kicking, ws_app
    ws_app = ws
    data = json.loads(message)
    t = data.get("t")
    op = data.get("op")

    if op == 0 and t == "MESSAGE_CREATE":
        msg = data["d"]
        author_id = msg["author"]["id"]
        content = msg["content"]
        channel_id = msg["channel_id"]
        guild_id = msg.get("guild_id")
        message_id = msg["id"]

        if author_id == str(OWNER_ID) and content.startswith(PREFIX):
            args = content[len(PREFIX):].split()
            cmd = args[0].lower()

            if cmd == 'ping':
                send_message(channel_id, '🏓 Pong!')
            elif cmd == 'say':
                send_message(channel_id, ' '.join(args[1:]))
            elif cmd == 'kickall':
                target_guild = guild_id
                if len(args) > 1:
                    target_guild = args[1]
                if not target_guild:
                    send_message(channel_id, "❌ No server ID. Use `!kickall 123456789`")
                    return
                if kicking:
                    send_message(channel_id, "❌ Already kicking!")
                    return
                delete_message(channel_id, message_id)
                send_message(channel_id, f'👢 Starting kickall for server `{target_guild}`...')
                threading.Thread(target=kick_all, args=(target_guild, channel_id)).start()
            elif cmd == 'help':
                send_message(channel_id, "!ping !say !kickall [server_id] !help")

    elif op == 0 and t == "GUILD_MEMBERS_CHUNK":
        on_guild_members_chunk(data["d"])

def on_error(ws, error): print(error)
def on_close(ws, *args):
    print("Reconnect...")
    time.sleep(5)
    connect_ws()
def on_open(ws):
    global ws_app
    ws_app = ws
    print("✅ Connected")
    # NO intents for user accounts
    ws.send(json.dumps({
        "op": 2,
        "d": {
            "token": TOKEN,
            "properties": {"os": "linux", "browser": "chrome", "device": "chrome"},
            "presence": {"status": "online", "afk": False}
        }
    }))

def connect_ws():
    global ws_app
    ws = websocket.WebSocketApp(
        "wss://gateway.discord.gg/?v=10&encoding=json",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws_app = ws
    ws.run_forever()

print("🚀 Starting (no intents, user account)...")
connect_ws()
